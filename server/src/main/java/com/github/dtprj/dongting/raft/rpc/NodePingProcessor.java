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
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.impl.NodeManager;

/**
 * @author huangli
 */
public class NodePingProcessor extends ReqProcessor<NodePing> {

    private final int selfNodeId;
    private final NodeManager nodeManager;

    public NodePingProcessor(int selfNodeId, NodeManager nodeManager) {
        this.selfNodeId = selfNodeId;
        this.nodeManager = nodeManager;
    }

    @Override
    public WritePacket process(ReadPacket<NodePing> packet, ReqContext reqContext) {
        DtUtil.SCHEDULED_SERVICE.execute(() -> {
            NodePing reqPing = packet.getBody();
            WritePacket p;
            if (!nodeManager.containsNode(reqPing.localNodeId)) {
                p = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
                p.msg = "node not found: " + reqPing.localNodeId;
            } else {
                NodePing respPing = new NodePing(selfNodeId, reqPing.localNodeId, nodeManager.getUuid());
                p = new SimpleWritePacket(respPing);
                p.respCode = CmdCodes.SUCCESS;
            }
            reqContext.writeRespInBizThreads(p);
        });
        return null;
    }

    @Override
    public DecoderCallback<NodePing> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new NodePing());
    }
}
