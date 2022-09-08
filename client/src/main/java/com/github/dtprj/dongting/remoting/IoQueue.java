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
package com.github.dtprj.dongting.remoting;

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
    private final ConcurrentLinkedQueue<WriteObj> writeQueue = new ConcurrentLinkedQueue<>();

    public IoQueue() {
    }

    public void write(WriteObj data) {
        writeQueue.add(data);
    }

    public boolean dispatchWriteQueue(HashMap<Integer, WriteObj> pendingRequests) {
        ConcurrentLinkedQueue<WriteObj> writeQueue = this.writeQueue;
        WriteObj wo;
        boolean result = false;
        while ((wo = writeQueue.poll()) != null) {
            Frame req = wo.getData();
            DtChannel dtc = wo.getDtc();
            if (req.getFrameType() == CmdType.TYPE_REQ) {
                int seq = dtc.getAndIncSeq();
                req.setSeq(seq);
                WriteObj old = pendingRequests.put(seq, wo);
                if (old != null) {
                    String errMsg = "dup seq: old=" + old.getData() + ", new=" + req;
                    log.error(errMsg);
                    wo.getFuture().completeExceptionally(new RemotingException(errMsg));
                    pendingRequests.put(req.getSeq(), old);
                    continue;
                }
            }
            dtc.getSubQueue().enqueue(req.toByteBuffer());
            result = true;
        }
        return result;
    }
}
