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

import com.github.dtprj.dongting.codec.CodecException;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.raft.RaftNode;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author huangli
 */
public class AdminListNodesResp extends PbCallback<AdminListNodesResp> implements Encodable {
    private static final int IDX_SIZE = 1;
    private static final int IDX_NODE = 2;

    public List<RaftNode> nodes;
    private PbCallback<RaftNode> nodeCallback;

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        if (nodes == null) {
            return true;
        }
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_SIZE, nodes.size())) {
                    return false;
                }
                // fall through
            case IDX_SIZE:
                return EncodeUtil.encodeList(context, destBuffer, IDX_NODE, nodes);
            default:
                throw new CodecException(context);
        }
    }

    @Override
    public int actualSize() {
        if (nodes == null) {
            return 0;
        }
        return PbUtil.sizeOfInt32Field(IDX_SIZE, nodes.size()) +
                EncodeUtil.sizeOfList(IDX_NODE, nodes);
    }

    @Override
    protected AdminListNodesResp getResult() {
        return this;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        if (index == IDX_SIZE) {
            nodes = createArrayList((int) value);
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index == IDX_NODE) {
            if (nodeCallback == null) {
                nodeCallback = new RaftNode.Callback();
            }
            RaftNode r = parseNested(buf, fieldLen, currentPos, nodeCallback);
            if (r != null) {
                nodes.add(r);
            }
        }
        return true;
    }
}
