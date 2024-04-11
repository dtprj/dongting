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
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.server.RaftServer;

/**
 * @author huangli
 */
public class RaftPingProcessor extends RaftSequenceProcessor<RaftPingFrameCallback> {
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
    protected FiberFrame<Void> processInFiberGroup(ReqInfoEx<RaftPingFrameCallback> reqInfo) {
        GroupComponents gc = reqInfo.getRaftGroup().getGroupComponents();
        RaftPingWriteFrame resp = new RaftPingWriteFrame(gc.getGroupConfig().getGroupId(),
                gc.getServerConfig().getNodeId(), gc.getRaftStatus().getNodeIdOfMembers(),
                gc.getRaftStatus().getNodeIdOfObservers());
        resp.setRespCode(CmdCodes.SUCCESS);
        writeResp(reqInfo, resp);
        return FiberFrame.voidCompletedFrame();
    }

    @Override
    public Decoder<RaftPingFrameCallback> createDecoder(int command) {
        return DECODER;
    }
}
