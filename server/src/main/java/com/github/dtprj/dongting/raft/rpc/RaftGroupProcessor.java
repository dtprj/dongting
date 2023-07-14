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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespFrame;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftServer;

/**
 * @author huangli
 */
public abstract class RaftGroupProcessor<T> extends ReqProcessor<T> {
    private static final DtLog log = DtLogs.getLogger(RaftGroupProcessor.class);
    private final boolean runInCurrentThread;

    private final RaftServer raftServer;

    public RaftGroupProcessor(boolean runInCurrentThread, RaftServer raftServer) {
        this.runInCurrentThread = runInCurrentThread;
        this.raftServer = raftServer;
    }

    protected abstract int getGroupId(ReadFrame<T> frame);

    protected abstract WriteFrame doProcess(ReadFrame<T> frame, ChannelContext channelContext,
                                            ReqContext reqContext, RaftGroup raftGroup);

    @Override
    public final WriteFrame process(ReadFrame<T> frame, ChannelContext channelContext, ReqContext reqContext) {
        Object body = frame.getBody();
        if (body == null) {
            EmptyBodyRespFrame errorResp = new EmptyBodyRespFrame(CmdCodes.CLIENT_ERROR);
            errorResp.setMsg("empty body");
            return errorResp;
        }
        int groupId = getGroupId(frame);
        RaftGroupImpl gc = (RaftGroupImpl) raftServer.getRaftGroup(groupId);
        if (gc == null) {
            EmptyBodyRespFrame errorResp = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
            errorResp.setMsg("raft group not found: " + groupId);
            log.error(errorResp.getMsg());
            return errorResp;
        }

        RaftStatusImpl status = gc.getRaftStatus();
        if (status.isStop()) {
            EmptyBodyRespFrame wf = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
            wf.setMsg("raft group is stopped: " + groupId);
            return wf;
        } else if (status.isError()) {
            EmptyBodyRespFrame wf = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
            wf.setMsg("raft group is in error state: " + gc.getGroupId());
            return wf;
        } else {
            if (runInCurrentThread) {
                return doProcess(frame, channelContext, reqContext, gc);
            } else {
                gc.getRaftExecutor().execute(() -> process(frame, channelContext, reqContext, gc, status));
                return null;
            }
        }
    }

    private void process(ReadFrame<T> frame, ChannelContext channelContext, ReqContext reqContext,
                         RaftGroup rg, RaftStatusImpl status) {
        WriteFrame wf;
        if (status.isStop()) {
            wf = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
            wf.setMsg("raft group is stopped: " + rg.getGroupId());
        } else if (status.isError()) {
            wf = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
            wf.setMsg("raft group is in error state: " + rg.getGroupId());
        } else {
            wf = doProcess(frame, channelContext, reqContext, rg);
        }
        if (wf != null) {
            channelContext.getRespWriter().writeRespInBizThreads(frame, wf, reqContext.getTimeout());
        }
    }

}
