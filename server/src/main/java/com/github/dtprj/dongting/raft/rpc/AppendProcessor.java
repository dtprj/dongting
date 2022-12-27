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
package com.github.dtprj.dongting.raft.rpc;

import com.github.dtprj.dongting.net.Decoder;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.ProcessContext;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.raft.impl.RaftTask;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author huangli
 */
public class AppendProcessor extends ReqProcessor {

    private final LinkedBlockingQueue<RaftTask> raftThreadQueue;

    private PbZeroCopyDecoder decoder = new PbZeroCopyDecoder() {
        @Override
        protected PbCallback createCallback(ProcessContext context) {
            return new AppendReqCallback();
        }
    };

    public AppendProcessor(LinkedBlockingQueue<RaftTask> raftThreadQueue) {
        this.raftThreadQueue = raftThreadQueue;
    }

    @Override
    public WriteFrame process(ReadFrame frame, ProcessContext context) {
        RaftTask task = new RaftTask();
        task.setType(RaftTask.TYPE_APPEND_ENTRIES_REQ);
        task.setData(frame);
        task.setRespWriter(context.getRespWriter());
        raftThreadQueue.offer(task);
        return null;
    }

    @Override
    public Decoder getDecoder() {
        return decoder;
    }
}
