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

import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.codec.SimpleEncodable;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author huangli
 */
public class WatchReq implements SimpleEncodable {

    public static final int IDX_GROUP_ID = 1;
    public static final int IDX_OPERATION = 2;
    public static final int IDX_KEYS_SIZE = 3;
    public static final int IDX_KNOWN_RAFT_INDEXES = 3;
    public static final int IDX_KEYS = 4;

    public final int groupId;
    public final int operation;
    public final long[] knownRaftIndexes;
    public final byte[][] keys;

    public WatchReq(int groupId, int operation, long[] knownRaftIndexes, byte[][] keys) {
        Objects.requireNonNull(knownRaftIndexes);
        Objects.requireNonNull(keys);
        if (knownRaftIndexes.length != keys.length) {
            throw new IllegalArgumentException("knownRaftIndexes.length != keys.length");
        }
        this.groupId = groupId;
        this.operation = operation;
        this.knownRaftIndexes = knownRaftIndexes;
        this.keys = keys;
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeInt32Field(buf, IDX_GROUP_ID, groupId);
        PbUtil.writeInt32Field(buf, IDX_OPERATION, operation);
        PbUtil.writeInt32Field(buf, IDX_KEYS_SIZE, keys.length);
        PbUtil.writeFix64Field(buf, IDX_KNOWN_RAFT_INDEXES, knownRaftIndexes);
        PbUtil.writeBytesField(buf, IDX_KEYS, keys);
    }

    @Override
    public int actualSize() {
        return PbUtil.sizeOfInt32Field(IDX_GROUP_ID, groupId)
                + PbUtil.sizeOfInt32Field(IDX_OPERATION, operation)
                + PbUtil.sizeOfInt32Field(IDX_KEYS_SIZE, keys.length)
                + PbUtil.sizeOfFix64Field(IDX_KNOWN_RAFT_INDEXES, knownRaftIndexes)
                + PbUtil.sizeOfBytesField(IDX_KEYS, keys);
    }
}
