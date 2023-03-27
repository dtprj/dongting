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

import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Decoder;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.GroupComponentsMap;

/**
 * @author huangli
 */
public class RaftPingProcessor extends AbstractProcessor {
    public static final PbZeroCopyDecoder DECODER = new PbZeroCopyDecoder(context ->
            new RaftPingFrameCallback());

    public RaftPingProcessor(GroupComponentsMap groupComponentsMap) {
        super(groupComponentsMap);
    }

    @Override
    protected int getGroupId(ReadFrame frame) {
        return ((RaftPingFrameCallback) frame.getBody()).groupId;
    }

    @Override
    protected WriteFrame doProcess(ReadFrame frame, ChannelContext channelContext, GroupComponents gc) {
        RaftPingWriteFrame resp = new RaftPingWriteFrame(gc.getServerConfig().getNodeId(),
                gc.getGroupConfig().getGroupId(), gc.getRaftStatus().getNodeIdOfMembers(),
                gc.getRaftStatus().getNodeIdOfObservers());
        resp.setRespCode(CmdCodes.SUCCESS);
        return resp;
    }

    @Override
    public Decoder getDecoder() {
        return DECODER;
    }
}
