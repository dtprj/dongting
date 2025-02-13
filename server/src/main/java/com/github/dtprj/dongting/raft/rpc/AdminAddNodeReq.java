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

/**
 * @author huangli
 */
public class AdminAddNodeReq extends PbCallback<AdminAddNodeReq> implements SimpleEncodable {
    public int nodeId;
    public String host;
    public int port;

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeUnsignedInt32(buf, 1, nodeId);
        PbUtil.writeAscii(buf, 2, host);
        PbUtil.writeUnsignedInt32(buf, 3, port);
    }

    @Override
    public int actualSize() {
        return PbUtil.accurateUnsignedIntSize(1, nodeId) +
                PbUtil.accurateStrSizeAscii(2, host) +
                PbUtil.accurateUnsignedIntSize(3, port);
    }

    @Override
    protected AdminAddNodeReq getResult() {
        return this;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        if (index == 1) {
            nodeId = (int) value;
        } else if (index == 3) {
            port = (int) value;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index == 2) {
            host = parseUTF8(buf, fieldLen, currentPos);
        }
        return true;
    }
}
