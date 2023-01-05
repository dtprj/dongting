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

import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Decoder;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.ProcessContext;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.raft.impl.Raft;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatus;

/**
 * @author huangli
 */
public class AppendProcessor extends ReqProcessor {

    private final RaftStatus raftStatus;

    private PbZeroCopyDecoder decoder = new PbZeroCopyDecoder() {
        @Override
        protected PbCallback createCallback(ProcessContext context) {
            return new AppendReqCallback(context.getIoHeapBufferPool());
        }
    };

    public AppendProcessor(RaftStatus raftStatus) {
        this.raftStatus = raftStatus;
    }

    @Override
    public WriteFrame process(ReadFrame rf, ProcessContext context) {
        AppendRespWriteFrame resp = new AppendRespWriteFrame();
        AppendReqCallback req = (AppendReqCallback) rf.getBody();
        int remoteTerm = req.getTerm();
        RaftStatus raftStatus = this.raftStatus;
        int localTerm = raftStatus.getCurrentTerm();
        if (remoteTerm == localTerm) {
            if (raftStatus.getRole() == RaftRole.follower) {
                raftStatus.setLastLeaderActiveTime(System.nanoTime());
                resp.setSuccess(true);
            } else if (raftStatus.getRole() == RaftRole.candidate) {
                Raft.updateTermAndConvertToFollower(remoteTerm, raftStatus);
                resp.setSuccess(true);
            } else {
                BugLog.getLog().error("leader receive raft append request. term={}, remote={}",
                        remoteTerm, context.getRemoteAddr());
                resp.setSuccess(false);
            }
        } else if (remoteTerm > localTerm) {
            Raft.updateTermAndConvertToFollower(remoteTerm, raftStatus);
            resp.setSuccess(true);
        } else {
            resp.setSuccess(false);
        }
        resp.setTerm(raftStatus.getCurrentTerm());
        resp.setRespCode(CmdCodes.SUCCESS);
        return resp;
    }

    @Override
    public Decoder getDecoder() {
        return decoder;
    }
}
