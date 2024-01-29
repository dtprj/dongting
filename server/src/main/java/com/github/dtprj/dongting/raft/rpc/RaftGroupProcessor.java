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

import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
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

    protected abstract FiberFrame<Void> doProcess(ReqInfo<T> reqInfo);

    protected void writeResp(ReqInfo<?> reqInfo, WriteFrame respFrame) {
        reqInfo.channelContext.getRespWriter().writeRespInBizThreads(
                reqInfo.reqFrame, respFrame, reqInfo.reqContext.getTimeout());
    }

    public void startProcessFiber(FiberChannel<ReqInfo<T>> channel) {
        FiberFrame<Void> ff = new ProcessorFiberFrame(channel);
        Fiber f = new Fiber(getClass().getSimpleName(), FiberGroup.currentGroup(), ff, true);
        f.start();
    }

    private class ProcessorFiberFrame extends FiberFrame<Void> {

        private final FiberChannel<ReqInfo<T>> channel;
        private ReqInfo<T> current;

        ProcessorFiberFrame(FiberChannel<ReqInfo<T>> channel) {
            this.channel = channel;
        }

        @Override
        public FrameCallResult execute(Void input) {
            current = null;
            return channel.take(this::resume);
        }

        private FrameCallResult resume(ReqInfo<T> o) {
            current = o;
            return Fiber.call(doProcess(o), this);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            if (current != null) {
                EmptyBodyRespFrame wf = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
                wf.setMsg(ex.toString());
                current.getChannelContext().getRespWriter().writeRespInBizThreads(
                        current.getReqFrame(), wf, current.getReqContext().getTimeout());
            }
            if (!isGroupShouldStopPlain()) {
                log.warn("uncaught exception in {}, restart processor fiber: {}",
                        getClass().getSimpleName(), ex.toString());
                startProcessFiber(channel);
            }
            return Fiber.frameReturn();
        }
    }

    public static class ReqInfo<T> {
        private final ReadFrame<T> reqFrame;
        private final ChannelContext channelContext;
        private final ReqContext reqContext;
        private final RaftGroupImpl raftGroup;

        protected ReqInfo(ReadFrame<T> reqFrame, ChannelContext channelContext,
                          ReqContext reqContext, RaftGroupImpl raftGroup) {
            this.reqFrame = reqFrame;
            this.channelContext = channelContext;
            this.reqContext = reqContext;
            this.raftGroup = raftGroup;
        }

        public ReadFrame<T> getReqFrame() {
            return reqFrame;
        }

        public ChannelContext getChannelContext() {
            return channelContext;
        }

        public ReqContext getReqContext() {
            return reqContext;
        }

        public RaftGroupImpl getRaftGroup() {
            return raftGroup;
        }
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
            ReqInfo<?> reqInfo = new ReqInfo<>(frame, channelContext, reqContext, g);
            c.fireOffer(reqInfo);
            return null;
        }
    }

}
