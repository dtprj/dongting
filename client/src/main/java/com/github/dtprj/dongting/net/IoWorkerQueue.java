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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.PerfConsts;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.queue.MpscLinkedQueue;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
class IoWorkerQueue {
    private static final DtLog log = DtLogs.getLogger(IoWorkerQueue.class);
    private final MpscLinkedQueue<Object> queue = MpscLinkedQueue.newInstance();
    private final NioWorker worker;
    private final PerfCallback perfCallback;
    private int invokeIndex;

    public IoWorkerQueue(NioWorker worker, NioConfig config) {
        this.worker = worker;
        this.perfCallback = config.perfCallback;
    }

    public void writeFromBizThread(PacketInfo data) {
        data.perfTimeOrAddOrder = perfCallback.takeTime(PerfConsts.RPC_D_WORKER_QUEUE);
        if (!queue.offer(data)) {
            data.packet.clean();
            if (data instanceof PacketInfoReq) {
                ((PacketInfoReq) data).callFail(new NetException("IoQueue closed"));
            }
        }
    }

    public boolean scheduleFromBizThread(Runnable runnable) {
        return queue.offer(runnable);
    }

    public void dispatchActions() {
        Object data;
        while ((data = queue.relaxedPoll()) != null) {
            if (data instanceof PacketInfo) {
                processWriteData((PacketInfo) data);
            } else {
                ((Runnable) data).run();
            }
        }
    }

    public boolean dispatchFinished() {
        return queue.isConsumeFinished();
    }

    private void processWriteData(PacketInfo wo) {
        perfCallback.fireTime(PerfConsts.RPC_D_WORKER_QUEUE, wo.perfTimeOrAddOrder);
        DtChannelImpl dtc = wo.dtc;
        if (dtc != null) {
            if (dtc.isClosed()) {
                wo.packet.clean();
                if (wo instanceof PacketInfoReq) {
                    ((PacketInfoReq) wo).callFail(new NetException("channel closed during dispatch"));
                }
            } else {
                dtc.subQueue.enqueue(wo);
            }
            return;
        }
        PacketInfoReq pir = (PacketInfoReq) wo;
        Peer peer = pir.peer;
        if (peer != null) {
            if (peer.status == PeerStatus.connected) {
                dtc = peer.dtChannel;
                pir.dtc = dtc;
                dtc.subQueue.enqueue(pir);
            } else if (peer.status == PeerStatus.removed) {
                pir.packet.clean();
                pir.callFail(new NetException("peer is removed"));
            } else {
                peer.addToWaitConnectList(pir);
                if (peer.status == PeerStatus.not_connect) {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    worker.doConnect(f, peer, new DtTime(10, TimeUnit.SECONDS), false);
                }
            }
        } else {
            if (!worker.server && pir.packet.packetType != PacketType.TYPE_RESP) {
                dtc = selectChannel();
                if (dtc == null) {
                    pir.packet.clean();
                    pir.callFail(new NetException("no available channel"));
                } else {
                    pir.dtc = dtc;
                    dtc.subQueue.enqueue(pir);
                }
            } else {
                log.error("no peer set");
                pir.packet.clean();
                pir.callFail(new NetException("no peer set"));
            }
        }
    }

    private DtChannelImpl selectChannel() {
        ArrayList<DtChannelImpl> list = worker.channelsList;
        @SuppressWarnings("DataFlowIssue") int size = list.size();
        if (size == 0) {
            return null;
        }
        int idx = invokeIndex;
        if (idx >= size) {
            idx = 0;
        }
        while (idx < size) {
            DtChannelImpl dtc = list.get(idx);
            if (dtc.handshake && dtc.getChannel().isOpen()) {
                invokeIndex = idx + 1;
                return dtc;
            }
            idx++;
        }
        return null;
    }

    public void close() {
        queue.shutdown();
    }
}
