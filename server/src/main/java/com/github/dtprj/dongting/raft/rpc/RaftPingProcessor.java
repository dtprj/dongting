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

import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Decoder;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.MemberManager;
import com.github.dtprj.dongting.raft.server.GroupComponents;

/**
 * @author huangli
 */
public class RaftPingProcessor extends ReqProcessor {
    public static final PbZeroCopyDecoder DECODER = new PbZeroCopyDecoder(context ->
            new RaftPingFrameCallback());

    private final IntObjMap<GroupComponents> groupComponents;

    public RaftPingProcessor(IntObjMap<GroupComponents> groupComponents) {
        this.groupComponents = groupComponents;
    }

    @Override
    public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
        RaftPingFrameCallback callback = (RaftPingFrameCallback) frame.getBody();
        GroupComponents gc = groupComponents.get(callback.groupId);
        RaftPingWriteFrame resp;
        if (gc == null) {
            resp = new RaftPingWriteFrame(0, 0, null);
        } else {
            MemberManager mm = gc.getMemberManager();
            resp = new RaftPingWriteFrame(gc.getServerConfig().getId(),
                    gc.getGroupConfig().getGroupId(), mm.getIds());
        }
        resp.setRespCode(CmdCodes.SUCCESS);
        return resp;
    }

    @Override
    public Decoder getDecoder() {
        return DECODER;
    }
}
