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
public class RespWriter {

    private final IoWorkerQueue ioWorkerQueue;
    private final Runnable wakeupRunnable;
    private final DtChannelImpl dtc;

    RespWriter(IoWorkerQueue ioWorkerQueue, Runnable wakeupRunnable, DtChannelImpl dtc) {
        this.ioWorkerQueue = ioWorkerQueue;
        this.wakeupRunnable = wakeupRunnable;
        this.dtc = dtc;
    }

    // invoke by other threads
    public void writeRespInBizThreads(ReadPacket<?> req, WritePacket resp, DtTime timeout) {
        resp.setSeq(req.getSeq());
        resp.setCommand(req.getCommand());
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

        resp.setPacketType(PacketType.TYPE_RESP);
        WriteData data = new WriteData(dtc, resp, timeout);
        ioWorkerQueue.writeFromBizThread(data);
        wakeupRunnable.run();
    }
}
