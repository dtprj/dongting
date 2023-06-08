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

import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.SmallNoCopyWriteFrame;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author huangli
 */
public class NodePingWriteFrame extends SmallNoCopyWriteFrame {

    private final int selfNodeId;
    private final UUID uuid;

    public NodePingWriteFrame(int selfNodeId, UUID uuid) {
        this.selfNodeId = selfNodeId;
        this.uuid = uuid;
        setCommand(Commands.NODE_PING);
    }

    @Override
    protected int calcActualBodySize() {
        return PbUtil.accurateFix32Size(1, selfNodeId)
                + PbUtil.accurateFix64Size(2, uuid.getMostSignificantBits())
                + PbUtil.accurateFix64Size(3, uuid.getLeastSignificantBits());
    }

    @Override
    protected void encodeBody(ByteBuffer buf) {
        PbUtil.writeFix32(buf, 1, selfNodeId);
        PbUtil.writeFix64(buf, 2, uuid.getMostSignificantBits());
        PbUtil.writeFix64(buf, 3, uuid.getLeastSignificantBits());
    }
}
