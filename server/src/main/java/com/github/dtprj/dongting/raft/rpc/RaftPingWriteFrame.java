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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.pb.PbUtil;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * @author huangli
 */
public class RaftPingWriteFrame extends WriteFrame {

    private final int groupId;
    private final int nodeId;
    private final Set<Integer> groupIds;

    public RaftPingWriteFrame(int groupId, int nodeId, Set<Integer> groupIds) {
        this.groupId = groupId;
        this.nodeId = nodeId;
        this.groupIds = groupIds;
    }

    @Override
    protected int calcEstimateBodySize() {
        int size = PbUtil.accurateFix32Size(1, groupId);
        size += PbUtil.accurateFix32Size(2, nodeId);
        if (groupIds != null) {
            for (int id : groupIds) {
                size += PbUtil.accurateFix32Size(3, id);
            }
        }
        return size;
    }

    @Override
    protected void encodeBody(ByteBuffer buf, ByteBufferPool pool) {
        super.writeBodySize(buf, estimateBodySize());
        PbUtil.writeFix32(buf, 1, groupId);
        PbUtil.writeFix32(buf, 2, nodeId);
        if (groupIds != null) {
            for (int id : groupIds) {
                PbUtil.writeFix32(buf, 2, id);
            }
        }
    }
}
