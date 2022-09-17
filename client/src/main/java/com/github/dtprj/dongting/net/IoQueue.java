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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author huangli
 */
// TODO currently just work, optimize performance
class IoQueue {
    private static final DtLog log = DtLogs.getLogger(IoQueue.class);
    private final ConcurrentLinkedQueue<WriteData> writeQueue = new ConcurrentLinkedQueue<>();

    public IoQueue() {
    }

    public void write(WriteData data) {
        writeQueue.add(data);
    }

    public boolean dispatchWriteQueue(HashMap<Integer, WriteData> pendingRequests) {
        ConcurrentLinkedQueue<WriteData> writeQueue = this.writeQueue;
        WriteData wo;
        boolean result = false;
        while ((wo = writeQueue.poll()) != null) {
            WriteFrame frame = wo.getData();
            DtChannel dtc = wo.getDtc();
            if (dtc == null) {
                // dtc may update
                dtc = wo.getEndPoint().getDtChannel();
            }
            if (frame.getFrameType() == CmdType.TYPE_REQ) {
                int seq = dtc.getAndIncSeq();
                frame.setSeq(seq);
                WriteData old = pendingRequests.put(seq, wo);
                if (old != null) {
                    String errMsg = "dup seq: old=" + old.getData() + ", new=" + frame;
                    log.error(errMsg);
                    wo.getFuture().completeExceptionally(new NetException(errMsg));
                    pendingRequests.put(frame.getSeq(), old);
                    continue;
                }
            }
            dtc.getSubQueue().enqueue(frame);
            result = true;
        }
        return result;
    }
}
