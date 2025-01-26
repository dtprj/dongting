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

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftServer;

/**
 * @author huangli
 */
public class RaftPingProcessor extends RaftSequenceProcessor<RaftPing> {

    public RaftPingProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    @Override
    protected int getGroupId(ReadPacket<RaftPing> frame) {
        return frame.getBody().groupId;
    }

    @Override
    protected FiberFrame<Void> processInFiberGroup(ReqInfoEx<RaftPing> reqInfo) {
        GroupComponents gc = reqInfo.raftGroup.getGroupComponents();
        SimpleWritePacket resp = RaftUtil.buildRaftPingPacket(gc.getServerConfig().getNodeId(), gc.getRaftStatus());
        resp.setRespCode(CmdCodes.SUCCESS);
        writeResp(reqInfo, resp);
        return FiberFrame.voidCompletedFrame();
    }

    @Override
    public DecoderCallback<RaftPing> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new RaftPing());
    }
}
