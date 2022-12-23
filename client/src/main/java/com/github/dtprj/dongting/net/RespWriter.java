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

/**
 * @author huangli
 */
public class RespWriter {

    private final IoQueue ioQueue;
    private final Runnable wakeupRunnable;
    private final DtChannel dtc;

    RespWriter(IoQueue ioQueue, Runnable wakeupRunnable, DtChannel dtc) {
        this.ioQueue = ioQueue;
        this.wakeupRunnable = wakeupRunnable;
        this.dtc = dtc;
    }

    // invoke by other threads
    public void writeRespInBizThreads(WriteFrame resp) {
        if (dtc.isClosed()) {
            // not restrict, but we will check again in io thread
            return;
        }
        resp.setFrameType(FrameType.TYPE_RESP);
        WriteData data = new WriteData(dtc, resp);
        ioQueue.writeFromBizThread(data);
        wakeupRunnable.run();
    }

    // invoke by other threads
    public void writeRespInBizThreads(Frame req, WriteFrame resp) {
        resp.setSeq(req.getSeq());
        resp.setCommand(req.getCommand());
        writeRespInBizThreads(resp);
    }
}
