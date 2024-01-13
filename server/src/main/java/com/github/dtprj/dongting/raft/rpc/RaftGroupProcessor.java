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

import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
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
import com.github.dtprj.dongting.raft.server.RaftServer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author huangli
 */
public abstract class RaftGroupProcessor<T> extends ReqProcessor<T> {
    private static final DtLog log = DtLogs.getLogger(RaftGroupProcessor.class);

    private static final AtomicInteger PROCESSOR_TYPE_ID = new AtomicInteger();

    private final int typeId = PROCESSOR_TYPE_ID.incrementAndGet();

    private final RaftServer raftServer;

    public RaftGroupProcessor(RaftServer raftServer) {
        this.raftServer = raftServer;
    }

    protected abstract int getGroupId(ReadFrame<T> frame);

    public FiberFrame<Void> createFiberFrame(FiberChannel<Object> channel) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return channel.take(this::resume);
            }

            private FrameCallResult resume(Object o) {
                ReqInfo reqInfo = (ReqInfo) o;
                return doProcess(reqInfo.reqFrame, reqInfo.channelContext, reqInfo.reqContext, reqInfo.raftGroup);
            }
        };
    }

    protected abstract FrameCallResult doProcess(ReadFrame<T> reqFrame, ChannelContext channelContext,
                                                 ReqContext reqContext, RaftGroupImpl raftGroup);

    private static class ReqInfo {
        ReadFrame reqFrame;
        ChannelContext channelContext;
        ReqContext reqContext;
        RaftGroupImpl raftGroup;
    }

    /**
     * run in io thread.
     */
    @Override
    public final WriteFrame process(ReadFrame<T> frame, ChannelContext channelContext, ReqContext reqContext) {
        Object body = frame.getBody();
        if (body == null) {
            EmptyBodyRespFrame errorResp = new EmptyBodyRespFrame(CmdCodes.CLIENT_ERROR);
            errorResp.setMsg("empty body");
            return errorResp;
        }
        int groupId = getGroupId(frame);
        RaftGroupImpl g = (RaftGroupImpl) raftServer.getRaftGroup(groupId);
        if (g == null) {
            EmptyBodyRespFrame errorResp = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
            errorResp.setMsg("raft group not found: " + groupId);
            log.error(errorResp.getMsg());
            return errorResp;
        }
        GroupComponents gc = g.getGroupComponents();
        if (gc.getFiberGroup().isShouldStop()) {
            EmptyBodyRespFrame wf = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
            wf.setMsg("raft group is stopped: " + groupId);
            return wf;
        } else if (gc.getRaftStatus().isError()) {
            EmptyBodyRespFrame wf = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
            wf.setMsg("raft group is in error state: " + g.getGroupId());
            return wf;
        } else {
            FiberChannel<Object> c = g.getProcessorChannels().get(typeId);
            ReqInfo reqInfo = new ReqInfo();
            reqInfo.reqFrame = frame;
            reqInfo.channelContext = channelContext;
            reqInfo.reqContext = reqContext;
            reqInfo.raftGroup = g;
            c.fireOffer(reqInfo);
            return null;
        }
    }

}
