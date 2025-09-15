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

    private boolean iteratingPendingQueue;
    private final LongObjMap<WriteData> pendingRequests = new LongObjMap<>();

    private final ArrayList<WriteData> tempSortList = new ArrayList<>();

    private static final long NEAR_TIMEOUT_THRESHOLD_NANOS = 800_000_000L;
    private long lastCleanTimeNanos;
    private WriteData nearTimeoutQueueHead;
    private WriteData nearTimeoutQueueTail;

    private long addOrder;

    public WorkerStatus(NioWorker worker, IoWorkerQueue ioWorkerQueue, ByteBufferPool directPool,
                        RefBufferFactory heapPool, Timestamp ts) {
        this.worker = worker;
        this.ioWorkerQueue = ioWorkerQueue;
        this.directPool = directPool;
        this.heapPool = heapPool;
        this.ts = ts;
        this.lastCleanTimeNanos = ts.nanoTime;
    }

    public void addPacketsToWrite(int delta) {
        this.packetsToWrite = Math.max(packetsToWrite + delta, 0);
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


    private void addToNearTimeoutQueueIfNeed(WriteData wd) {
        if (wd.timeout.getDeadlineNanos() - ts.nanoTime < NEAR_TIMEOUT_THRESHOLD_NANOS) {
            // add to near timeout queue
            if (nearTimeoutQueueHead == null) {
                nearTimeoutQueueHead = wd;
            } else {
                nearTimeoutQueueTail.nearTimeoutQueueNext = wd;
                wd.nearTimeoutQueuePrev = nearTimeoutQueueTail;
            }
            nearTimeoutQueueTail = wd;
        }
    }

    private void removeFromNearTimeoutQueue(WriteData wd) {
        WriteData prev = wd.nearTimeoutQueuePrev;
        WriteData next = wd.nearTimeoutQueueNext;
        if (prev == null && next == null && nearTimeoutQueueHead != wd) {
            // not in near timeout queue
            return;
        }

        if (prev != null) {
            prev.nearTimeoutQueueNext = next;
        } else {
            nearTimeoutQueueHead = next;
        }

        if (next != null) {
            next.nearTimeoutQueuePrev = prev;
        } else {
            nearTimeoutQueueTail = prev;
        }

        wd.nearTimeoutQueueNext = null;
        wd.nearTimeoutQueuePrev = null;
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
            removeFromChannelQueue(old);
            removeFromNearTimeoutQueue(old);
            old.callFail(true, new NetException(errMsg));
        }
        addToNearTimeoutQueueIfNeed(wd);

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
            removeFromChannelQueue(o);
            removeFromNearTimeoutQueue(o);
        }
        return o;
    }

    public int pendingReqSize() {
        return pendingRequests.size();
    }

    public void cleanAllPendingReq() {
        if (iteratingPendingQueue) {
            throw new IllegalStateException("iteratingPendingQueue");
        }
        iteratingPendingQueue = true;
        ArrayList<WriteData> list = this.tempSortList;
        try {
            pendingRequests.forEach((key, wd) -> {
                removeFromChannelQueue(wd);
                removeFromNearTimeoutQueue(wd);
                list.add(wd);
                return false;
            });
            if (list.isEmpty()) {
                return;
            }
            list.sort(this::compare);
            NetException e = new NetException("NioWorker closed");
            for (WriteData wd : list) {
                // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
                // so callFail should be idempotent
                wd.callFail(false, e);
            }
        } finally {
            iteratingPendingQueue = false;
            list.clear();
        }
    }

    private int compare(WriteData a, WriteData b) {
        // addOrder may overflow, so we use subtraction to compare, can't use < or >
        long x = a.addOrder - b.addOrder;
        return x < 0 ? -1 : (x == 0 ? 0 : 1);
    }

    public void cleanPendingReqByChannel(DtChannelImpl dtc) {
        if (iteratingPendingQueue) {
            throw new IllegalStateException("iteratingPendingQueue");
        }
        iteratingPendingQueue = true;
        try {
            NetException e = null;
            while (dtc.pendingReqHead != null) {
                WriteData wd = dtc.pendingReqHead;
                pendingRequests.remove(BitUtil.toLong(dtc.channelIndexInWorker, wd.data.seq));
                removeFromChannelQueue(wd);
                if (e == null) {
                    e = new NetException("channel closed, cancel pending request in NioWorker");
                }
                // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
                // so callFail should be idempotent
                wd.callFail(false, e);
            }
        } finally {
            iteratingPendingQueue = false;
        }
    }

    public void cleanPendingReqByTimeout() {
        if (iteratingPendingQueue) {
            throw new IllegalStateException("iteratingPendingQueue");
        }
        iteratingPendingQueue = true;

        ArrayList<WriteData> list = this.tempSortList;
        try {
            Timestamp ts = this.ts;
            LongObjMap<WriteData> pendingRequests = this.pendingRequests;
            if (ts.nanoTime - lastCleanTimeNanos > NEAR_TIMEOUT_THRESHOLD_NANOS) {
                // this iterate is o(n), so not do it too frequently
                pendingRequests.forEach((key, wd) -> {
                    addToNearTimeoutQueueIfNeed(wd);
                });
                lastCleanTimeNanos = ts.nanoTime;
            }

            while (nearTimeoutQueueHead != null) {
                WriteData wd = nearTimeoutQueueHead;
                if (wd.timeout.isTimeout(ts)) {
                    pendingRequests.remove(BitUtil.toLong(wd.dtc.channelIndexInWorker, wd.data.seq));
                    removeFromChannelQueue(wd);
                    removeFromNearTimeoutQueue(wd);
                    list.add(wd);
                } else {
                    break;
                }
            }
            if (list.isEmpty()) {
                return;
            }

            list.sort(this::compare);
            for (WriteData wd : list) {
                WritePacket wp = wd.data;
                long timeout = wd.timeout.getTimeout(TimeUnit.MILLISECONDS);
                if (log.isDebugEnabled()) {
                    log.debug("drop timeout request: {}ms, cmd={}, seq={}, {}", timeout, wp.command,
                            wp.seq, wd.dtc.getChannel());
                }
                String msg = "request is timeout: " + timeout + "ms, cmd=" + wp.command
                        + ", remote=" + wd.dtc.getRemoteAddr();
                // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
                // so callFail should be idempotent
                wd.callFail(false, new NetTimeoutException(msg));
            }
        } finally {
            iteratingPendingQueue = false;
            list.clear();
        }
    }

}
