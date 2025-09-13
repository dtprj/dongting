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
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.common.Timestamp;
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

    // TODO iterate this by channel or timeout is o(n)
    private final LongObjMap<WriteData> pendingRequests = new LongObjMap<>();
    private final ArrayList<WriteData> tempSortList = new ArrayList<>();

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

    public WriteData addPendingReq(WriteData wd) {
        long key = BitUtil.toLong(wd.dtc.channelIndexInWorker, wd.data.seq);
        return pendingRequests.put(key, wd);
    }

    public WriteData removePendingReq(int channelIndex, int seq) {
        return pendingRequests.remove(BitUtil.toLong(channelIndex, seq));
    }

    public int pendingReqSize() {
        return pendingRequests.size();
    }

    public void cleanAllPendingReq() {
        pendingRequests.forEach((key, wd) -> {
            if (wd.callback != null) {
                wd.callFail(false, new NetException("NioWorker closed"));
            }
            return false;
        });
    }

    public void cleanPendingReqByChannel(DtChannelImpl dtc) {
        ArrayList<WriteData> list = this.tempSortList;
        try {
            pendingRequests.forEach((noUsedKey, wd) -> {
                if (wd.dtc == dtc) {
                    if (wd.callback != null) {
                        list.add(wd);
                    }
                    return false;
                } else {
                    return true;
                }
            });

            if (list.isEmpty()) {
                return;
            }
            list.sort(this::compare);
            for (WriteData wd : list) {
                // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
                // so callFail should be idempotent
                wd.callFail(false, new NetException("channel closed, cancel pending request in NioWorker"));
            }
        } finally {
            list.clear();
        }
    }

    public void cleanPendingReqByTimeout() {
        ArrayList<WriteData> list = this.tempSortList;
        try {
            pendingRequests.forEach((noUsedKey, wd) -> {
                DtTime t = wd.timeout;
                if (wd.dtc.isClosed() || t.isTimeout(ts)) {
                    if (wd.callback != null) {
                        list.add(wd);
                    }
                    return false;
                } else {
                    return true;
                }
            });

            if (list.isEmpty()) {
                return;
            }
            list.sort(this::compare);
            for (WriteData wd : list) {
                // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
                // so callFail should be idempotent

                DtTime t = wd.timeout;
                if (wd.dtc.isClosed()) {
                    wd.callFail(false, new NetException("channel closed, future cancelled by timeout cleaner"));
                } else if (t.isTimeout(ts)) {
                    WritePacket wp = wd.data;
                    long timeout = t.getTimeout(TimeUnit.MILLISECONDS);
                    log.debug("drop timeout request: {}ms, cmd={}, seq={}, {}", timeout, wp.command,
                            wp.seq, wd.dtc.getChannel());
                    String msg = "request is timeout: " + timeout + "ms, cmd=" + wp.command
                            + ", remote=" + wd.dtc.getRemoteAddr();
                    wd.callFail(false, new NetTimeoutException(msg));
                }
            }
        } finally {
            list.clear();
        }
    }

    private int compare(WriteData a, WriteData b) {
        // we only need keep order in same connection, so we only use seq.
        // seq may overflow, so we use subtraction to compare, can't use < or >
        return a.data.seq - b.data.seq;
    }

}
