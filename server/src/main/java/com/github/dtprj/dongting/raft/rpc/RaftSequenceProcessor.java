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
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author huangli
 */
public abstract class RaftSequenceProcessor<T> extends RaftProcessor<T> {
    private static final DtLog log = DtLogs.getLogger(RaftSequenceProcessor.class);

    private static final AtomicInteger PROCESSOR_TYPE_ID = new AtomicInteger();

    private final int typeId = PROCESSOR_TYPE_ID.incrementAndGet();

    public RaftSequenceProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    protected abstract FiberFrame<Void> processInFiberGroup(ReqInfoEx<T> reqInfo);

    public final int getTypeId() {
        return typeId;
    }

    public void startProcessFiber(FiberChannel<ReqInfoEx<T>> channel) {
        FiberFrame<Void> ff = new ProcessorFiberFrame(channel);
        Fiber f = new Fiber(getClass().getSimpleName(), FiberGroup.currentGroup(), ff, true);
        f.start();
    }

    private class ProcessorFiberFrame extends FiberFrame<Void> {

        private final FiberChannel<ReqInfoEx<T>> channel;
        private ReqInfo<T> current;

        ProcessorFiberFrame(FiberChannel<ReqInfoEx<T>> channel) {
            this.channel = channel;
        }

        @Override
        public FrameCallResult execute(Void input) {
            current = null;
            return channel.take(this::resume);
        }

        private FrameCallResult resume(ReqInfoEx<T> o) {
            if (o == null) {
                return Fiber.resume(null, this);
            }
            current = o;
            return Fiber.call(processInFiberGroup(o), this);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            if (current != null) {
                EmptyBodyRespPacket wf = new EmptyBodyRespPacket(CmdCodes.BIZ_ERROR);
                wf.setMsg(ex.toString());
                current.getReqContext().getDtChannel().getRespWriter().writeRespInBizThreads(
                        current.getReqFrame(), wf, current.getReqContext().getTimeout());
            }
            if (!isGroupShouldStopPlain()) {
                log.error("uncaught exception in {}, restart processor fiber: {}",
                        getClass().getSimpleName(), ex.toString());
                startProcessFiber(channel);
            }
            return Fiber.frameReturn();
        }
    }

    @Override
    protected final WritePacket doProcess(ReqInfo<T> reqInfo) {
        ReqInfoEx<T> rix = (ReqInfoEx<T>) reqInfo;
        FiberChannel<Object> c = rix.getRaftGroup().getGroupComponents().getProcessorChannels().get(typeId);
        if (!c.fireOffer(reqInfo)) {
            invokeCleanReqInProcessorThread(reqInfo);
            log.error("fire task failed , maybe group is stopped: {}", reqInfo.getRaftGroup().getGroupId());
        }
        return null;
    }
}
