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
    private final LongObjMap<PacketInfo> pendingRequests = new LongObjMap<>();

    private final ArrayList<PacketInfo> tempSortList = new ArrayList<>();

    private final long nearTimeoutThresholdNanos;
    private long lastCleanTimeNanos;
    private PacketInfo nearTimeoutQueueHead;
    private PacketInfo nearTimeoutQueueTail;

    private long addOrder;

    public WorkerStatus(NioWorker worker, IoWorkerQueue ioWorkerQueue, ByteBufferPool directPool,
                        RefBufferFactory heapPool, Timestamp ts, long nearTimeoutThresholdMillis) {
        this.worker = worker;
        this.ioWorkerQueue = ioWorkerQueue;
        this.directPool = directPool;
        this.heapPool = heapPool;
        this.ts = ts;
        this.lastCleanTimeNanos = ts.nanoTime;
        this.nearTimeoutThresholdNanos = TimeUnit.MILLISECONDS.toNanos(nearTimeoutThresholdMillis);
    }

    public void addPacketsToWrite(int delta) {
        this.packetsToWrite = Math.max(packetsToWrite + delta, 0);
    }

    private void removeFromChannelQueue(PacketInfo pi) {
        DtChannelImpl dtc = pi.dtc;
        PacketInfo prev = pi.prevInChannel;
        PacketInfo next = pi.nextInChannel;

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

        pi.nextInChannel = null;
        pi.prevInChannel = null;
    }


    private void addToNearTimeoutQueueIfNeed(PacketInfo pi) {
        if (pi.timeout.deadlineNanos - ts.nanoTime < nearTimeoutThresholdNanos) {
            // add to near timeout queue
            if (nearTimeoutQueueHead == null) {
                nearTimeoutQueueHead = pi;
            } else {
                nearTimeoutQueueTail.nearTimeoutQueueNext = pi;
                pi.nearTimeoutQueuePrev = nearTimeoutQueueTail;
            }
            nearTimeoutQueueTail = pi;
        }
    }

    private void removeFromNearTimeoutQueue(PacketInfo pi) {
        PacketInfo prev = pi.nearTimeoutQueuePrev;
        PacketInfo next = pi.nearTimeoutQueueNext;
        if (prev == null && next == null && nearTimeoutQueueHead != pi) {
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

        pi.nearTimeoutQueueNext = null;
        pi.nearTimeoutQueuePrev = null;
    }

    public void addPendingReq(PacketInfo pi) {
        pi.perfTimeOrAddOrder = addOrder;
        addOrder++;
        DtChannelImpl dtc = pi.dtc;

        PacketInfo old = pendingRequests.put(BitUtil.toLong(dtc.channelIndexInWorker, pi.packet.seq), pi);
        if (old != null) {
            // seq overflow and reuse, or bug
            String errMsg = "dup seq: old=" + old.packet + ", new=" + pi.packet;
            BugLog.getLog().error(errMsg);
            removeFromChannelQueue(old);
            removeFromNearTimeoutQueue(old);
            old.callFail(true, new NetException(errMsg));
        }
        addToNearTimeoutQueueIfNeed(pi);

        // add to channel queue
        if (dtc.pendingReqHead == null) {
            dtc.pendingReqHead = pi;
        } else {
            dtc.pendingReqTail.nextInChannel = pi;
            pi.prevInChannel = dtc.pendingReqTail;
        }
        dtc.pendingReqTail = pi;
    }

    public PacketInfo removePendingReq(int channelIndex, int seq) {
        PacketInfo o = pendingRequests.remove(BitUtil.toLong(channelIndex, seq));
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
        ArrayList<PacketInfo> list = this.tempSortList;
        try {
            pendingRequests.forEach((key, pi) -> {
                removeFromChannelQueue(pi);
                removeFromNearTimeoutQueue(pi);
                list.add(pi);
                return false;
            });
            if (list.isEmpty()) {
                return;
            }
            list.sort(this::compare);
            NetException e = new NetException("NioWorker closed");
            for (PacketInfo pi : list) {
                // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
                // so callFail should be idempotent
                pi.callFail(false, e);
            }
        } finally {
            iteratingPendingQueue = false;
            list.clear();
        }
    }

    private int compare(PacketInfo a, PacketInfo b) {
        // addOrder may overflow, so we use subtraction to compare, can't use < or >
        long x = a.perfTimeOrAddOrder - b.perfTimeOrAddOrder;
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
                PacketInfo pi = dtc.pendingReqHead;
                pendingRequests.remove(BitUtil.toLong(dtc.channelIndexInWorker, pi.packet.seq));
                removeFromChannelQueue(pi);
                if (e == null) {
                    e = new NetException("channel closed, cancel pending request in NioWorker");
                }
                // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
                // so callFail should be idempotent
                pi.callFail(false, e);
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

        ArrayList<PacketInfo> list = this.tempSortList;
        try {
            Timestamp ts = this.ts;
            LongObjMap<PacketInfo> pendingRequests = this.pendingRequests;
            if (ts.nanoTime - lastCleanTimeNanos > nearTimeoutThresholdNanos) {
                // this iterate is o(n), so not do it too frequently
                pendingRequests.forEach((key, pi) -> {
                    addToNearTimeoutQueueIfNeed(pi);
                });
                lastCleanTimeNanos = ts.nanoTime;
            }

            while (nearTimeoutQueueHead != null) {
                PacketInfo pi = nearTimeoutQueueHead;
                if (pi.timeout.isTimeout(ts)) {
                    pendingRequests.remove(BitUtil.toLong(pi.dtc.channelIndexInWorker, pi.packet.seq));
                    removeFromChannelQueue(pi);
                    removeFromNearTimeoutQueue(pi);
                    list.add(pi);
                } else {
                    break;
                }
            }
            if (list.isEmpty()) {
                return;
            }

            list.sort(this::compare);
            for (PacketInfo pi : list) {
                WritePacket p = pi.packet;
                long timeout = pi.timeout.getTimeout(TimeUnit.MILLISECONDS);
                if (log.isDebugEnabled()) {
                    log.debug("drop timeout request: {}ms, cmd={}, seq={}, {}", timeout, p.command,
                            p.seq, pi.dtc.getChannel());
                }
                String msg = "request is timeout: " + timeout + "ms, cmd=" + p.command
                        + ", remote=" + pi.dtc.getRemoteAddr();
                // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
                // so callFail should be idempotent
                pi.callFail(false, new NetTimeoutException(msg));
            }
        } finally {
            iteratingPendingQueue = false;
            list.clear();
        }
    }

}
