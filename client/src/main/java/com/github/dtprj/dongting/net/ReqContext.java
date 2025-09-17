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

/**
 * @author huangli
 */
public class ReqContext {
    private final DtChannelImpl dtc;
    private final ReadPacket<?> req;
    private final DtTime timeout;

    ReqContext(DtChannelImpl dtc, ReadPacket<?> req, DtTime timeout) {
        this.dtc = dtc;
        this.req = req;
        this.timeout = timeout;
    }

    public DtTime getTimeout() {
        return timeout;
    }

    public DtChannel getDtChannel() {
        return dtc;
    }

    // invoke by other threads
    public void writeRespInBizThreads(WritePacket resp) {
        resp.seq = req.seq;
        resp.command = req.command;
        resp.packetType = PacketType.TYPE_RESP;

        PacketInfo data = new PacketInfo(dtc, resp, timeout);
        NioWorker worker = dtc.workerStatus.worker;
        if (Thread.currentThread() == worker.thread) {
            dtc.subQueue.enqueue(data);
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

            worker.workerStatus.ioWorkerQueue.writeFromBizThread(data);
            worker.wakeup();
        }
    }
}
