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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.queue.MpscLinkedQueue;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
class IoQueue {
    private static final DtLog log = DtLogs.getLogger(IoQueue.class);
    private final MpscLinkedQueue<Object> queue = MpscLinkedQueue.newInstance();
    private final NioWorker worker;
    private int invokeIndex;

    public IoQueue(NioWorker worker) {
        this.worker = worker;
    }

    public void writeFromBizThread(WriteData data) {
        if (!queue.offer(data)) {
            if (data.getFuture() != null) {
                data.getFuture().completeExceptionally(new NetException("IoQueue closed"));
            }
        }
    }

    public void scheduleFromBizThread(Runnable runnable) throws NetException {
        if (!queue.offer(runnable)) {
            throw new NetException("IoQueue closed");
        }
    }

    public void dispatchActions() {
        Object data;
        while ((data = queue.relaxedPoll()) != null) {
            if (data instanceof WriteData) {
                processWriteData((WriteData) data);
            } else {
                ((Runnable) data).run();
            }
        }
    }

    private void processWriteData(WriteData wo) {
        WriteFrame frame = wo.getData();
        Peer peer = wo.getPeer();
        if (peer != null) {
            if (peer.getStatus() == PeerStatus.connected) {
                DtChannel dtc = peer.getDtChannel();
                wo.setDtc(dtc);
                dtc.getSubQueue().enqueue(wo);
            } else if (peer.getStatus() == PeerStatus.removed) {
                if (wo.getFuture() != null) {
                    frame.clean();
                    wo.getFuture().completeExceptionally(new NetException("peer is removed"));
                }
            } else {
                peer.addToWaitConnectList(wo);
                if (peer.getStatus() == PeerStatus.not_connect) {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    worker.doConnect(f, peer, new DtTime(10, TimeUnit.SECONDS));
                }
            }
        } else {
            DtChannel dtc = wo.getDtc();
            if (dtc == null) {
                if (!worker.isServer() && frame.getFrameType() == FrameType.TYPE_REQ) {
                    dtc = selectChannel();
                    if (dtc == null) {
                        frame.clean();
                        wo.getFuture().completeExceptionally(new NetException("no available channel"));
                    } else {
                        wo.setDtc(dtc);
                        dtc.getSubQueue().enqueue(wo);
                    }
                } else {
                    log.error("no peer set");
                    if (frame.getFrameType() == FrameType.TYPE_REQ) {
                        frame.clean();
                        wo.getFuture().completeExceptionally(new NetException("no peer set"));
                    }
                }
            } else {
                if (dtc.isClosed()) {
                    if (wo.getFuture() != null) {
                        frame.clean();
                        wo.getFuture().completeExceptionally(new NetException("channel closed during dispatch"));
                    }
                } else {
                    dtc.getSubQueue().enqueue(wo);
                }
            }
        }
    }

    private DtChannel selectChannel() {
        ArrayList<DtChannel> list = worker.getChannelsList();
        int size = list.size();
        if (size == 0) {
            return null;
        }
        int idx = invokeIndex;
        if (idx < size) {
            invokeIndex = idx + 1;
            return list.get(idx);
        } else {
            invokeIndex = 0;
            return list.get(0);
        }
    }

    public void close() {
        queue.shutdownByConsumer();
    }
}
