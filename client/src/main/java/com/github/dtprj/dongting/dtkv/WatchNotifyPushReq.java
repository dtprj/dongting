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
package com.github.dtprj.dongting.dtkv;

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.codec.SimpleEncodable;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class WatchNotifyPushReq implements SimpleEncodable {
    public static final int IDX_GROUP_ID = 1;
    public static final int IDX_RAFT_INDEX = 2;
    public static final int IDX_RESULT = 3;
    public static final int IDX_KEY = 4;
    public static final int IDX_VALUE = 5;

    public final int groupId;
    public final long raftIndex;
    public final int result;
    public final byte[] key;
    public final byte[] value;

    public WatchNotifyPushReq(int groupId, long raftIndex, int result, byte[] key, byte[] value) {
        this.groupId = groupId;
        this.raftIndex = raftIndex;
        this.result = result;
        this.key = key;
        this.value = value;
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeInt32Field(buf, IDX_GROUP_ID, groupId);
        PbUtil.writeFix64Field(buf, IDX_RAFT_INDEX, raftIndex);
        PbUtil.writeInt32Field(buf, IDX_RESULT, result);
        PbUtil.writeBytesField(buf, IDX_KEY, key);
        PbUtil.writeBytesField(buf, IDX_VALUE, value);
    }

    @Override
    public int actualSize() {
        return PbUtil.sizeOfInt32Field(IDX_GROUP_ID, groupId)
                + PbUtil.sizeOfFix64Field(IDX_RAFT_INDEX, raftIndex)
                + PbUtil.sizeOfInt32Field(IDX_RESULT, result)
                + PbUtil.sizeOfBytesField(IDX_KEY, key)
                + PbUtil.sizeOfBytesField(IDX_VALUE, value);
    }

    public static class Callback extends PbCallback<WatchNotifyPushReq> {
        private int groupId;
        private long raftIndex;
        private int result;
        private byte[] key;
        private byte[] value;

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == IDX_GROUP_ID) {
                groupId = (int) value;
            } else if (index == IDX_RESULT) {
                result = (int) value;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            if(index == IDX_RAFT_INDEX) {
                raftIndex = value;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == IDX_KEY) {
                key = parseBytes(buf, fieldLen, currentPos);
            } else if (index == IDX_VALUE) {
                value = parseBytes(buf, fieldLen, currentPos);
            }
            return true;
        }

        @Override
        protected WatchNotifyPushReq getResult() {
            return new WatchNotifyPushReq(groupId, raftIndex, result, key, value);
        }
    }
}
