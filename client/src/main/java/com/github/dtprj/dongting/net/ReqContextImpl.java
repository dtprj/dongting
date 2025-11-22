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

/**
 * @author huangli
 */
class ReqContextImpl extends PacketInfo implements ReqContext, Runnable {
    private static final DtLog log = DtLogs.getLogger(ReqContextImpl.class);

    private final ReadPacket<?> req;
    private final ReqProcessor<?> processor;
    private final int reqPacketSize;
    private final boolean reqFlowControl;

    ReqContextImpl(DtChannelImpl dtc, ReadPacket<?> req, DtTime timeout, ReqProcessor<?> processor,
                   int reqPacketSize, boolean reqFlowControl) {
        super(dtc, null, timeout);
        this.req = req;
        this.processor = processor;
        this.reqPacketSize = reqPacketSize;
        this.reqFlowControl = reqFlowControl;
    }

    @Override
    public DtTime getTimeout() {
        return timeout;
    }

    @Override
    public DtChannel getDtChannel() {
        return dtc;
    }

    @Override
    // invoke by other threads
    public void writeRespInBizThreads(WritePacket resp) {
        resp.seq = req.seq;
        resp.command = req.command;
        resp.packetType = PacketType.TYPE_RESP;

        this.packet = resp;

        NioWorker worker = dtc.workerStatus.worker;
        if (Thread.currentThread() == worker.thread) {
            dtc.subQueue.enqueue(this);
            worker.markWakeupInIoThread();
        } else {
            if (dtc.isClosed()) {
                resp.clean();
                // not restrict, but we will check again in io thread
                return;
            }
            if (req.responseHasWrite) {
                // this check is not thread safe
                throw new IllegalStateException("the response has been written");
            }
            req.responseHasWrite = true;

            worker.workerStatus.ioWorkerQueue.writeFromBizThread(this);
            worker.wakeup();
        }
    }

    @Override
    public void run() {
        WritePacket resp = null;
        @SuppressWarnings("rawtypes") ReadPacket req = this.req;
        DtChannelImpl dtc = this.dtc;
        try {
            if (DtChannelImpl.timeout(req, this, null)) {
                return;
            }
            //noinspection unchecked
            resp = processor.process(req, this);
        } catch (NetCodeException e) {
            log.warn("ReqProcessor.process fail, command={}, code={}, msg={}", req.command, e.getCode(), e.getMessage());
            if (req.packetType == PacketType.TYPE_REQ) {
                resp = new EmptyBodyRespPacket(e.getCode());
                resp.msg = e.toString();
            }
        } catch (Throwable e) {
            log.warn("ReqProcessor.process fail, command={}", req.command, e);
            if (req.packetType == PacketType.TYPE_REQ) {
                resp = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
                resp.msg = e.toString();
            }
        } finally {
            if (reqFlowControl) {
                dtc.releasePending(reqPacketSize);
            }
        }
        if (resp != null) {
            writeRespInBizThreads(resp);
        }
    }
}
