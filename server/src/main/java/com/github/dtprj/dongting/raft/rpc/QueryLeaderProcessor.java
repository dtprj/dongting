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
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.SimpleStrWriteFrame;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.RaftMember;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftGroupProcessor;

/**
 * @author huangli
 */
public class QueryLeaderProcessor extends RaftGroupProcessor<Integer> {

    @Override
    public Decoder<Integer> createDecoder() {
        return PbNoCopyDecoder.SIMPLE_INT_DECODER;
    }

    @Override
    protected int getGroupId(ReadFrame<Integer> frame) {
        return frame.getBody();
    }

    @Override
    protected WriteFrame doProcess(ReadFrame<Integer> frame, ChannelContext channelContext, RaftGroup rg) {
        RaftStatusImpl raftStatus = (RaftStatusImpl) rg.getRaftStatus();
        RaftMember leader = raftStatus.getCurrentLeader();
        if (leader == null) {
            return new SimpleStrWriteFrame(null);
        } else {
            HostPort hp = leader.getNode().getPeer().getEndPoint();
            return new SimpleStrWriteFrame(hp.getHost() + ":" + hp.getPort());
        }
    }
}
