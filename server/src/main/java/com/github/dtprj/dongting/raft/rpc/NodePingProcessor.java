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
import com.github.dtprj.dongting.codec.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WriteFrame;

import java.util.UUID;

/**
 * @author huangli
 */
public class NodePingProcessor extends ReqProcessor<NodePingCallback> {

    public static final PbZeroCopyDecoder<NodePingCallback> DECODER = new PbZeroCopyDecoder<>(ctx -> new NodePingCallback());
    private final int selfNodeId;
    private final UUID uuid;

    public NodePingProcessor(int selfNodeId, UUID uuid) {
        this.selfNodeId = selfNodeId;
        this.uuid = uuid;
    }

    @Override
    public WriteFrame process(ReadFrame<NodePingCallback> frame, ChannelContext channelContext, ReqContext reqContext) {
        return new NodePingWriteFrame(selfNodeId, uuid);
    }

    @Override
    public Decoder<NodePingCallback> getDecoder() {
        return DECODER;
    }
}
