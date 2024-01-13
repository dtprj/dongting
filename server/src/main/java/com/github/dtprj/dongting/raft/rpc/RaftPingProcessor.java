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

import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.PbNoCopyDecoder;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.server.RaftServer;

/**
 * @author huangli
 */
public class RaftPingProcessor extends RaftGroupProcessor<RaftPingFrameCallback> {
    public static final PbNoCopyDecoder<RaftPingFrameCallback> DECODER = new PbNoCopyDecoder<>(context ->
            new RaftPingFrameCallback());

    public RaftPingProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    @Override
    protected int getGroupId(ReadFrame<RaftPingFrameCallback> frame) {
        return frame.getBody().groupId;
    }

    @Override
    protected FrameCallResult doProcess(ReadFrame<RaftPingFrameCallback> reqFrame, ChannelContext channelContext,
                                        ReqContext reqContext, RaftGroupImpl rg) {
        GroupComponents gc = rg.getGroupComponents();
        RaftPingWriteFrame resp = new RaftPingWriteFrame(gc.getServerConfig().getNodeId(),
                gc.getGroupConfig().getGroupId(), gc.getRaftStatus().getNodeIdOfMembers(),
                gc.getRaftStatus().getNodeIdOfObservers());
        resp.setRespCode(CmdCodes.SUCCESS);
        channelContext.getRespWriter().writeRespInBizThreads(reqFrame, resp, reqContext.getTimeout());
        return Fiber.frameReturn();
    }

    @Override
    public Decoder<RaftPingFrameCallback> createDecoder() {
        return DECODER;
    }
}
