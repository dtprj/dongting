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

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.codec.SimpleEncodable;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author huangli
 */
public class NodePing extends PbCallback<NodePing> implements SimpleEncodable {
    public int nodeId;
    public long uuidHigh;
    public long uuidLow;

    public NodePing() {
    }

    public NodePing(int nodeId, UUID uuid) {
        this.nodeId = nodeId;
        this.uuidHigh = uuid.getMostSignificantBits();
        this.uuidLow = uuid.getLeastSignificantBits();
    }

    @Override
    public int actualSize() {
        return PbUtil.accurateFix32Size(1, nodeId)
                + PbUtil.accurateFix64Size(2, uuidHigh)
                + PbUtil.accurateFix64Size(3, uuidLow);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeFix32(buf, 1, nodeId);
        PbUtil.writeFix64(buf, 2, uuidHigh);
        PbUtil.writeFix64(buf, 3, uuidLow);
    }

    @Override
    public boolean readFix32(int index, int value) {
        if (index == 1) {
            this.nodeId = value;
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        if (index == 2) {
            this.uuidHigh = value;
        } else if (index == 3) {
            this.uuidLow = value;
        }
        return true;
    }

    @Override
    public NodePing getResult() {
        return this;
    }
}