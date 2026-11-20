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
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtmq.codec.MqPullReq;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.WorkerThread;
import com.github.dtprj.dongting.raft.impl.RaftCancelException;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.rpc.RaftSequenceProcessor;
import com.github.dtprj.dongting.raft.rpc.ReqInfoEx;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

final class MqPullProcessor extends RaftSequenceProcessor<MqPullReq> {
    MqPullProcessor(RaftServer raftServer) {
        super(raftServer, true, false);
    }

    @Override
    public DecoderCallback<MqPullReq> createDecoderCallback(int cmd, DecodeContext context) {
        return context.toDecoderCallback(new MqPullReqCallback());
    }

    @Override
    protected int getGroupId(MqPullReq body) {
        return body.groupId;
    }

    /**
     * Run in io thread. If it returns true, the lease is not valid yet and the request ownership
     * is transferred: this processor waits for group ready, re-checks the lease, and starts
     * the pull fiber in the raft thread.
     */
    @Override
    protected boolean preProcess(ReqInfoEx<MqPullReq> reqInfo) {
        return !leaseCheck(reqInfo);
    }

    // run in raft thread
    @Override
    protected FiberFrame<Void> processInFiberGroup(ReqInfoEx<MqPullReq> reqInfo) {
        startPullFiber(reqInfo, true);
        return FiberFrame.voidCompletedFrame();
    }

    // run in io thread, or in raft thread when the group ready future completes.
    // returns true if the lease is valid; false means the request ownership is kept by this method
    private boolean leaseCheck(ReqInfoEx<MqPullReq> reqInfo) {
        RaftGroupImpl group = reqInfo.raftGroup;
        // the group ready callback may run in io thread if the future is already done
        Thread t = Thread.currentThread();
        Timestamp ts = t == group.fiberGroup.thread ? group.raftStatus.ts : ((WorkerThread) t).ts;
        try {
            if (group.isLeaseReadValid(ts, reqInfo.reqContext.getTimeout())) {
                return true;
            }
            group.addGroupReadyListener(reqInfo.reqContext.getTimeout()).whenComplete((v, ex) -> {
                if (ex == null) {
                    // follower also completes the ready future, so re-check lease here.
                    // the callback may run in io thread, so dispatch via fireFiber
                    if (leaseCheck(reqInfo)) {
                        startPullFiber(reqInfo, false);
                    }
                } else {
                    reqInfo.reqFrame.clean();
                    writeErrorResp(reqInfo, ex);
                }
            });
        } catch (Exception e) {
            reqInfo.reqFrame.clean();
            writeErrorResp(reqInfo, e);
        }
        return false;
    }

    // if the caller is not in the raft thread, the fiber is dispatched to the raft thread via fireFiber
    private void startPullFiber(ReqInfoEx<MqPullReq> reqInfo, boolean inRaftThread) {
        DtMQ mq = MqServerUtil.getStateMachine(reqInfo);
        if (mq == null) {
            reqInfo.reqFrame.clean();
            return;
        }
        FiberGroup fg = reqInfo.raftGroup.fiberGroup;
        Fiber pullFiber = new Fiber("mqPull", fg, new PullFrame(reqInfo, mq));
        if (inRaftThread) {
            pullFiber.start();
        } else if (!fg.fireFiber(pullFiber)) {
            reqInfo.reqFrame.clean();
            reqInfo.reqContext.writeRespInBizThreads(createStoppedResp(reqInfo.raftGroup.getGroupId()));
        }
    }

    private final class PullFrame extends FiberFrame<Void> {
        private final ReqInfo<MqPullReq> reqInfo;
        private final DtMQ mq;

        private PullFrame(ReqInfo<MqPullReq> reqInfo, DtMQ mq) {
            this.reqInfo = reqInfo;
            this.mq = mq;
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (!reqInfo.reqContext.getDtChannel().getChannel().isOpen()) {
                return Fiber.frameReturn();
            }
            MqPullFrame pull;
            MqPullReq req = reqInfo.reqFrame.getBody();
            try {
                pull = mq.pull(req.queueIds, req.fromSeqs, req.limit, req.bytesLimit,
                        req.waitMillis, reqInfo.reqContext.getTimeout(), reqInfo.reqContext.getDtChannel());
            } catch (IllegalArgumentException e) {
                reqInfo.reqContext.writeRespInBizThreads(
                        new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR, e.getMessage()));
                return Fiber.frameReturn();
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
                    reqInfo.reqContext.writeRespInBizThreads(
                            new EmptyBodyRespPacket(CmdCodes.STOPPING, "mq pull cancelled"));
                } else {
                    writeErrorResp(reqInfo, ex);
                }
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
