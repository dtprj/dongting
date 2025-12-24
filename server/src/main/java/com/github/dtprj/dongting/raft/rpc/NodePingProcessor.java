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
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.impl.NodeManager;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

/**
 * @author huangli
 */
public class NodePingProcessor extends ReqProcessor<NodePing> {

    private final NodeManager nodeManager;
    private final RaftServerConfig raftServerConfig;

    public NodePingProcessor(NodeManager nodeManager, RaftServerConfig raftServerConfig) {
        this.nodeManager = nodeManager;
        this.raftServerConfig = raftServerConfig;
    }

    @Override
    public WritePacket process(ReadPacket<NodePing> packet, ReqContext reqContext) {
        boolean servicePort = RaftPingProcessor.requestServicePort(reqContext, raftServerConfig);
        if (!RaftPingProcessor.checkPort(servicePort, false, true)) {
            packet.clean();
            return RaftPingProcessor.createWrongPortRest(packet, reqContext);
        }
        nodeManager.processNodePing(packet, reqContext);
        return null;
    }

    @Override
    public DecoderCallback<NodePing> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new NodePing());
    }
}
