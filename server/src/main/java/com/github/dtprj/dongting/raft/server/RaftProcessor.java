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
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.RaftTimeoutException;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.rpc.ReqInfoEx;

import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public abstract class RaftProcessor<T> extends ReqProcessor<T> {
    private static final DtLog log = DtLogs.getLogger(RaftProcessor.class);

    protected final RaftServer raftServer;

    public RaftProcessor(RaftServer raftServer) {
        this.raftServer = raftServer;
    }

    protected abstract int getGroupId(ReadPacket<T> frame);

    /**
     * run in io thread.
     */
    @Override
    public final WritePacket process(ReadPacket<T> packet, ReqContext reqContext) {
        int groupId = getGroupId(packet);
        if (groupId < 0) {
            // can't find raft group in decode phrase, return -1
            EmptyBodyRespPacket errorResp = new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_NOT_FOUND);
            errorResp.setMsg("raft group not found: " + groupId);
            log.error(errorResp.getMsg());
            return errorResp;
        }
        RaftGroupImpl g = (RaftGroupImpl) raftServer.getRaftGroup(groupId);
        ReqInfoEx<T> reqInfo = new ReqInfoEx<>(packet, reqContext, g);
        if (g == null) {
            invokeCleanReq(reqInfo);
            EmptyBodyRespPacket errorResp = new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_NOT_FOUND);
            errorResp.setMsg("raft group not found: " + groupId);
            log.error(errorResp.getMsg());
            return errorResp;
        }
        GroupComponents gc = g.getGroupComponents();
        if (!gc.getRaftStatus().initialized) {
            invokeCleanReq(reqInfo);
            EmptyBodyRespPacket wf = new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_NOT_INIT);
            wf.setMsg("raft group not initialized: " + groupId);
            return wf;
        }
        if (gc.getFiberGroup().isShouldStop()) {
            invokeCleanReq(reqInfo);
            EmptyBodyRespPacket wf = new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_STOPPED);
            wf.setMsg("raft group is stopped: " + groupId);
            return wf;
        } else {
            // release in sub class
            return doProcess(reqInfo);
        }
    }

    // may be invoked in different threads
    protected final void invokeCleanReq(ReqInfo<T> reqInfo) {
        try {
            if (!reqInfo.invokeCleanUp) {
                reqInfo.invokeCleanUp = true;
                cleanReq(reqInfo);
            } else {
                BugLog.log(new Exception("invokeCleanUp already invoked"));
            }
        } catch (Throwable e) {
            log.error("clean up error", e);
        }
    }

    protected abstract WritePacket doProcess(ReqInfo<T> reqInfo);

    protected void cleanReq(ReqInfo<T> reqInfo) {
    }

    protected void writeErrorResp(ReqInfo<?> reqInfo, Throwable ex) {
        Throwable root = DtUtil.rootCause(ex);
        if (root instanceof RaftTimeoutException) {
            ReadPacket<?> reqFrame = reqInfo.reqFrame;
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
            errorResp = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
            log.warn("raft processor error", ex);
        }
        errorResp.setMsg(root.toString());
        reqInfo.reqContext.writeRespInBizThreads(errorResp);
    }
}
