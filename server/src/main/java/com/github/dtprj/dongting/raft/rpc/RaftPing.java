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
public class RaftPing extends PbCallback<RaftPing> implements SimpleEncodable {
    public int groupId;
    public int nodeId;
    public String members;
    public String observers;
    public String preparedMembers;
    public String preparedObservers;

    @Override
    public boolean readVarNumber(int index, long value) {
        if (index == 1) {
            this.groupId = (int) value;
        } else if (index == 2) {
            this.nodeId = (int) value;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index == 3) {
            members = parseUTF8(buf, fieldLen, currentPos);
        } else if (index == 4) {
            observers = parseUTF8(buf, fieldLen, currentPos);
        } else if (index == 5) {
            preparedMembers = parseUTF8(buf, fieldLen, currentPos);
        } else if (index == 6) {
            preparedObservers = parseUTF8(buf, fieldLen, currentPos);
        }
        return true;
    }

    @Override
    public int actualSize() {
        int size = PbUtil.accurateUnsignedIntSize(1, groupId);
        size += PbUtil.accurateUnsignedIntSize(2, nodeId);
        size += PbUtil.accurateStrSizeAscii(3, members);
        size += PbUtil.accurateStrSizeAscii(4, observers);
        size += PbUtil.accurateStrSizeAscii(5, preparedMembers);
        size += PbUtil.accurateStrSizeAscii(6, preparedObservers);
        return size;
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeUnsignedInt32(buf, 1, groupId);
        PbUtil.writeUnsignedInt32(buf, 2, nodeId);
        PbUtil.writeAscii(buf, 3, members);
        PbUtil.writeAscii(buf, 4, observers);
        PbUtil.writeAscii(buf, 5, preparedMembers);
        PbUtil.writeAscii(buf, 6, preparedObservers);
    }

    @Override
    public RaftPing getResult() {
        return this;
    }
}
