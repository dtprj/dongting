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
import com.github.dtprj.dongting.raft.impl.RaftGroups;

/**
 * @author huangli
 */
public abstract class RaftGroupProcessor<T> extends ReqProcessor<T> {
    private static final DtLog log = DtLogs.getLogger(RaftGroupProcessor.class);

    private final RaftGroups raftGroups;

    public RaftGroupProcessor(RaftGroups raftGroups) {
        this.raftGroups = raftGroups;
    }

    protected abstract int getGroupId(ReadFrame<T> frame);

    protected abstract WriteFrame doProcess(ReadFrame<T> frame, ChannelContext channelContext, RaftGroup gc);

    @Override
    public final WriteFrame process(ReadFrame<T> frame, ChannelContext channelContext, ReqContext reqContext) {
        int groupId = getGroupId(frame);
        RaftGroupImpl gc = raftGroups.get(groupId);
        if (gc == null) {
            log.error("raft group not found: {}", groupId);
            EmptyBodyRespFrame wf = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
            wf.setMsg("raft group not found: " + groupId);
            return wf;
        }

        if (gc.getRaftStatus().isStop()) {
            EmptyBodyRespFrame wf = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
            wf.setMsg("raft group is stopped: " + groupId);
            return wf;
        } else {
            gc.getRaftExecutor().execute(() -> process(frame, channelContext, reqContext, gc));
            return null;
        }
    }

    private void process(ReadFrame<T> frame, ChannelContext channelContext, ReqContext reqContext, RaftGroup rg) {
        WriteFrame wf = doProcess(frame, channelContext, rg);
        if (wf != null) {
            channelContext.getRespWriter().writeRespInBizThreads(frame, wf, reqContext.getTimeout());
        }
    }

}
