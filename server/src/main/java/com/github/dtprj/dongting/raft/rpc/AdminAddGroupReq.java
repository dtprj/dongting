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
public class AdminAddGroupReq extends PbCallback<AdminAddGroupReq> implements SimpleEncodable {
    public int groupId;
    public String nodeIdOfMembers;
    public String nodeIdOfObservers;

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeUnsignedInt32(buf, 1, groupId);
        PbUtil.writeAscii(buf, 2, nodeIdOfMembers);
        PbUtil.writeAscii(buf, 3, nodeIdOfObservers);
    }

    @Override
    public int actualSize() {
        return PbUtil.accurateUnsignedIntSize(1, groupId) +
                PbUtil.accurateStrSizeAscii(2, nodeIdOfMembers) +
                PbUtil.accurateStrSizeAscii(3, nodeIdOfObservers);
    }

    @Override
    protected AdminAddGroupReq getResult() {
        return this;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        if (index == 1) {
            groupId = (int) value;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index == 2) {
            nodeIdOfMembers = parseUTF8(buf, fieldLen, currentPos);
        } else if (index == 3) {
            nodeIdOfObservers = parseUTF8(buf, fieldLen, currentPos);
        }
        return true;
    }
}
