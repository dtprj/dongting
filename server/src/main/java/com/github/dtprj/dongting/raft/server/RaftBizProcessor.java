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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.FlowControlException;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.RaftTimeoutException;

import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public abstract class RaftBizProcessor<T> extends RaftProcessor<T> {

    private static final DtLog log = DtLogs.getLogger(RaftBizProcessor.class);

    public RaftBizProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    protected void writeErrorResp(ReqInfo<?> reqInfo, Throwable ex) {
        Throwable root = DtUtil.rootCause(ex);
        if (root instanceof RaftTimeoutException) {
            ReadPacket<?> reqFrame = reqInfo.getReqFrame();
            log.warn("raft operation timeout: command={}, seq={}", reqFrame.getCommand(), reqFrame.getSeq());
            return;
        }
        EmptyBodyRespPacket errorResp;
        if (root instanceof FlowControlException) {
            errorResp = new EmptyBodyRespPacket(CmdCodes.FLOW_CONTROL);
        } else if (root instanceof NotLeaderException) {
            errorResp = new EmptyBodyRespPacket(CmdCodes.NOT_RAFT_LEADER);
            RaftNode leader = ((NotLeaderException) root).getCurrentLeader();
            if (leader != null) {
                errorResp.setExtra(String.valueOf(leader.getNodeId()).getBytes(StandardCharsets.UTF_8));
            }
            log.warn("not leader, current leader is {}", leader);
        } else {
            errorResp = new EmptyBodyRespPacket(CmdCodes.BIZ_ERROR);
            log.warn("raft processor error", ex);
        }
        errorResp.setMsg(root.toString());
        writeResp(reqInfo, errorResp);
    }
}
