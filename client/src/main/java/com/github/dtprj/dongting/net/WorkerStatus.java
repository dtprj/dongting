/*
 * Copyright The Dongting Project
 *
 * The Dongting Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
class WorkerStatus {
    private static final DtLog log = DtLogs.getLogger(WorkerStatus.class);
    final NioWorker worker;
    final IoWorkerQueue ioWorkerQueue;
    final ByteBufferPool directPool;
    final RefBufferFactory heapPool;
    final Timestamp ts;

    int retryConnect;
    int packetsToWrite;

    private final LongObjMap<WriteData> pendingRequests = new LongObjMap<>();
    private final PriorityQueue<WriteData> pendingTimeoutQueue = new PriorityQueue<>((o1, o2) -> {
        int x = o1.timeout.compareTo(o2.timeout);
        if (x != 0) {
            return x;
        } else {
            // timeout is same, use add order.
            // addOrder may overflow, so we use subtraction to compare, can't use < or >
            long y = o1.addOrder - o2.addOrder;
            return y < 0 ? -1 : (y == 0 ? 0 : 1);
        }
    });

    private long addOrder;

    public WorkerStatus(NioWorker worker, IoWorkerQueue ioWorkerQueue, ByteBufferPool directPool,
                        RefBufferFactory heapPool, Timestamp ts) {
        this.worker = worker;
        this.ioWorkerQueue = ioWorkerQueue;
        this.directPool = directPool;
        this.heapPool = heapPool;
        this.ts = ts;
    }

    public void addPacketsToWrite(int delta) {
        this.packetsToWrite = Math.max(packetsToWrite + delta, 0);
    }

    public void addPendingReq(WriteData wd) {
        wd.addOrder = addOrder;
        addOrder++;
        DtChannelImpl dtc = wd.dtc;

        WriteData old = pendingRequests.put(BitUtil.toLong(dtc.channelIndexInWorker, wd.data.seq), wd);
        if (old != null) {
            // seq overflow and reuse, or bug
            String errMsg = "dup seq: old=" + old.data + ", new=" + wd.data;
            BugLog.getLog().error(errMsg);
            pendingTimeoutQueue.remove(old);
            removeFromChannelQueue(old);
            old.callFail(true, new NetException(errMsg));
        }
        pendingTimeoutQueue.offer(wd);

        // add to channel queue
        if (dtc.pendingReqHead == null) {
            dtc.pendingReqHead = wd;
        } else {
            dtc.pendingReqTail.nextInChannel = wd;
            wd.prevInChannel = dtc.pendingReqTail;
        }
        dtc.pendingReqTail = wd;
    }

    public WriteData removePendingReq(int channelIndex, int seq) {
        WriteData o = pendingRequests.remove(BitUtil.toLong(channelIndex, seq));
        if (o != null) {
            pendingTimeoutQueue.remove(o);
            removeFromChannelQueue(o);
        }
        return o;
    }

    private void removeFromChannelQueue(WriteData wd) {
        DtChannelImpl dtc = wd.dtc;
        WriteData prev = wd.prevInChannel;
        WriteData next = wd.nextInChannel;

        if (prev != null) {
            prev.nextInChannel = next;
        } else {
            dtc.pendingReqHead = next;
        }

        if (next != null) {
            next.prevInChannel = prev;
        } else {
            dtc.pendingReqTail = prev;
        }

        wd.nextInChannel = null;
        wd.prevInChannel = null;
    }

    public int pendingReqSize() {
        return pendingRequests.size();
    }

    public void cleanAllPendingReq() {
        ArrayList<WriteData> tempSortList = new ArrayList<>(pendingRequests.size());
        pendingRequests.forEach((key, wd) -> {
            pendingTimeoutQueue.remove(wd);
            removeFromChannelQueue(wd);
            if (wd.callback != null) {
                tempSortList.add(wd);
            }
            return false;
        });
        if (tempSortList.isEmpty()) {
            return;
        }
        tempSortList.sort((a, b) -> {
            // addOrder may overflow, so we use subtraction to compare, can't use < or >
            long x = a.addOrder - b.addOrder;
            return x < 0 ? -1 : (x == 0 ? 0 : 1);
        });
        NetException e = new NetException("NioWorker closed");
        for (WriteData wd : tempSortList) {
            // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
            // so callFail should be idempotent
            wd.callFail(false, e);
        }
    }

    public void cleanPendingReqByChannel(DtChannelImpl dtc) {
        NetException e = null;
        while (dtc.pendingReqHead != null) {
            WriteData wd = dtc.pendingReqHead;
            pendingRequests.remove(BitUtil.toLong(dtc.channelIndexInWorker, wd.data.seq));
            pendingTimeoutQueue.remove(wd);
            removeFromChannelQueue(wd);
            if (e == null) {
                e = new NetException("channel closed, cancel pending request in NioWorker");
            }
            // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
            // so callFail should be idempotent
            wd.callFail(false, e);
        }
    }

    public void cleanPendingReqByTimeout() {
        Iterator<WriteData> it = pendingTimeoutQueue.iterator();
        NetTimeoutException e = null;
        while (it.hasNext()) {
            WriteData wd = it.next();
            if (wd.timeout.isTimeout(ts)) {
                WritePacket wp = wd.data;
                long timeout = wd.timeout.getTimeout(TimeUnit.MILLISECONDS);
                if (log.isDebugEnabled()) {
                    log.debug("drop timeout request: {}ms, cmd={}, seq={}, {}", timeout, wp.command,
                            wp.seq, wd.dtc.getChannel());
                }
                it.remove();
                pendingRequests.remove(BitUtil.toLong(wd.dtc.channelIndexInWorker, wp.seq));
                removeFromChannelQueue(wd);

                if (e == null) {
                    String msg = "request is timeout: " + timeout + "ms, cmd=" + wp.command
                            + ", remote=" + wd.dtc.getRemoteAddr();
                    e = new NetTimeoutException(msg);
                }

                // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
                // so callFail should be idempotent
                wd.callFail(false, e);
            } else {
                break;
            }
        }
    }

}
