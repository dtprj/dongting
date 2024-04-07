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

import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespFrame;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.rpc.ReqInfoEx;

/**
 * @author huangli
 */
public abstract class AbstractRaftGroupProcessor<T> extends ReqProcessor<T> {
    private static final DtLog log = DtLogs.getLogger(AbstractRaftGroupProcessor.class);

    protected final RaftServer raftServer;

    public AbstractRaftGroupProcessor(RaftServer raftServer) {
        this.raftServer = raftServer;
    }

    protected abstract int getGroupId(ReadFrame<T> frame);

    protected void writeResp(ReqInfo<?> reqInfo, WriteFrame respFrame) {
        reqInfo.getChannelContext().getRespWriter().writeRespInBizThreads(
                reqInfo.getReqFrame(), respFrame, reqInfo.getReqContext().getTimeout());
    }

    /**
     * run in io thread.
     */
    @Override
    public final WriteFrame process(ReadFrame<T> frame, ChannelContext channelContext, ReqContext reqContext) {
        Object body = frame.getBody();
        int groupId = getGroupId(frame);
        RaftGroupImpl g = (RaftGroupImpl) raftServer.getRaftGroup(groupId);
        ReqInfoEx<T> reqInfo = new ReqInfoEx<>(frame, channelContext, reqContext, g);
        if (body == null) {
            invokeCleanReqInProcessorThread(reqInfo);
            EmptyBodyRespFrame errorResp = new EmptyBodyRespFrame(CmdCodes.CLIENT_ERROR);
            errorResp.setMsg("empty body");
            return errorResp;
        }
        if (g == null) {
            invokeCleanReqInProcessorThread(reqInfo);
            EmptyBodyRespFrame errorResp = new EmptyBodyRespFrame(CmdCodes.RAFT_GROUP_NOT_FOUND);
            errorResp.setMsg("raft group not found: " + groupId);
            log.error(errorResp.getMsg());
            return errorResp;
        }
        GroupComponents gc = g.getGroupComponents();
        if (gc.getFiberGroup().isShouldStop()) {
            invokeCleanReqInProcessorThread(reqInfo);
            EmptyBodyRespFrame wf = new EmptyBodyRespFrame(CmdCodes.RAFT_GROUP_STOPPED);
            wf.setMsg("raft group is stopped: " + groupId);
            return wf;
        } else {
            // release in sub class
            return doProcess(reqInfo);
        }
    }

    protected final void invokeCleanReqInProcessorThread(ReqInfo<T> reqInfo) {
        try {
            if (!reqInfo.invokeCleanUp) {
                reqInfo.invokeCleanUp = true;
                cleanReqInProcessorThread(reqInfo);
            } else {
                BugLog.log(new Exception("invokeCleanUp already invoked"));
            }
        } catch (Throwable e) {
            log.error("clean up error", e);
        }
    }

    protected abstract WriteFrame doProcess(ReqInfo<T> reqInfo);

    protected abstract void cleanReqInProcessorThread(ReqInfo<T> reqInfo);
}
