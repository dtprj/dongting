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
package com.github.dtprj.dongting.dtmq.server;

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.dtmq.codec.MqPullReq;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.impl.RaftCancelException;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;

final class MqPullProcessor extends RaftProcessor<MqPullReq> {
    MqPullProcessor(RaftServer raftServer) {
        super(raftServer, true, false);
    }

    @Override
    public DecoderCallback<MqPullReq> createDecoderCallback(int cmd, DecodeContext context) {
        return context.toDecoderCallback(new MqPullReqCallback());
    }

    @Override
    protected int getGroupId(ReadPacket<MqPullReq> packet) {
        return packet.getBody().groupId;
    }

    @Override
    protected WritePacket doProcess(ReqInfo<MqPullReq> reqInfo) {
        StateMachine sm = reqInfo.raftGroup.getStateMachine();
        if (!(sm instanceof DtMQ)) {
            reqInfo.reqFrame.clean();
            EmptyBodyRespPacket resp = new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR);
            resp.msg = "not a dtmq group";
            return resp;
        }
        RaftGroupImpl group = (RaftGroupImpl) reqInfo.raftGroup;
        if (!group.fiberGroup.fireFiber("mqPull", new PullFrame(reqInfo, (DtMQ) sm))) {
            reqInfo.reqFrame.clean();
            return createStoppedResp(group.groupId);
        }
        return null;
    }

    private final class PullFrame extends FiberFrame<Void> {
        private final ReqInfo<MqPullReq> reqInfo;
        private final DtMQ mq;
        private MqPullFrame pull;

        private PullFrame(ReqInfo<MqPullReq> reqInfo, DtMQ mq) {
            this.reqInfo = reqInfo;
            this.mq = mq;
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (!reqInfo.reqContext.getDtChannel().getChannel().isOpen()) {
                return Fiber.frameReturn();
            }
            if (pull == null) {
                MqPullReq req = reqInfo.reqFrame.getBody();
                try {
                    pull = mq.pull(req.queueIds, req.fromSeqs, req.limit, req.bytesLimit,
                            req.waitMillis, reqInfo.reqContext.getTimeout(), reqInfo.reqContext.getDtChannel());
                } catch (IllegalArgumentException e) {
                    throw new NetCodeException(CmdCodes.CLIENT_ERROR, e.getMessage(), null);
                }
            }
            RaftGroupImpl group = (RaftGroupImpl) reqInfo.raftGroup;
            if (!group.isLeaseReadValid(group.groupComponents.raftStatus.ts, reqInfo.reqContext.getTimeout())) {
                FiberFuture<Void> ready = getFiberGroup().newFuture("mqPullReady");
                group.addGroupReadyListener(reqInfo.reqContext.getTimeout()).whenComplete((v, ex) -> {
                    if (ex == null) {
                        ready.fireComplete(null);
                    } else {
                        ready.fireCompleteExceptionally(ex);
                    }
                });
                return ready.await(this);
            }
            return Fiber.call(pull, this::afterPull);
        }

        private FrameCallResult afterPull(MqPullRespPacket resp) {
            resp.respCode = CmdCodes.SUCCESS;
            try {
                reqInfo.reqContext.writeRespInBizThreads(resp);
            } catch (Throwable e) {
                resp.clean();
                throw e;
            }
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            if (reqInfo.reqContext.getDtChannel().getChannel().isOpen()) {
                if (ex instanceof RaftCancelException) {
                    ex = new NetCodeException(CmdCodes.STOPPING, "mq pull cancelled", null);
                }
                writeErrorResp(reqInfo, ex);
            }
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult doFinally() {
            reqInfo.reqFrame.clean();
            return Fiber.frameReturn();
        }
    }
}
