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

import com.github.dtprj.dongting.codec.CodecException;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.raft.RaftRpcData;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

/**
 * @author huangli
 */
public class KvReq extends RaftRpcData implements Encodable {
    public static final int IDX_GROUP_ID = 1;
    public static final int IDX_KEY = 2;
    public static final int IDX_VALUE = 3;
    public static final int IDX_EXPECT_VALUE = 4;
    public static final int IDX_OWNER_UUID1 = 5;
    public static final int IDX_OWNER_UUID2 = 6;
    public static final int IDX_TTL_MILLIS = 7;
    public static final int IDX_KEYS_SIZE = 8;
    public static final int IDX_KEYS = 9;
    public static final int IDX_VALUES = 10;

    public byte[] key;
    public byte[] value;
    public List<byte[]> keys;
    public List<byte[]> values;
    public byte[] expectValue;
    public UUID ownerUuid;
    public long ttlMillis;

    private int encodeSize;

    public KvReq() {
    }

    public void checkKeysAndValues() {
        if (keys != null && values != null) {
            if (keys.size() != values.size()) {
                throw new IllegalArgumentException("keys and values size mismatch");
            }
        }
    }

    public KvReq(int groupId, byte[] key, byte[] value) {
        this.groupId = groupId;
        this.key = key;
        this.value = value;
        checkKeysAndValues();
    }

    public KvReq(int groupId, byte[] key, byte[] value, long ttlMillis) {
        this.groupId = groupId;
        this.key = key;
        this.value = value;
        this.ttlMillis = ttlMillis;
        checkKeysAndValues();
    }

    public KvReq(int groupId, byte[] key, byte[] value, byte[] expectValue) {
        this.groupId = groupId;
        this.key = key;
        this.value = value;
        this.expectValue = expectValue;
        checkKeysAndValues();
    }

    public KvReq(int groupId, List<byte[]> keys, List<byte[]> values) {
        this.groupId = groupId;
        this.keys = keys;
        this.values = values;
        checkKeysAndValues();
    }

    @Override
    public int actualSize() {
        if (encodeSize == 0) {
            encodeSize = PbUtil.sizeOfInt32Field(IDX_GROUP_ID, groupId)
                    + EncodeUtil.sizeOf(IDX_KEY, key)
                    + EncodeUtil.sizeOf(IDX_VALUE, value)
                    + EncodeUtil.sizeOf(IDX_EXPECT_VALUE, expectValue)
                    + PbUtil.sizeOfFix64Field(IDX_OWNER_UUID1, ownerUuid == null ? 0 : ownerUuid.getMostSignificantBits())
                    + PbUtil.sizeOfFix64Field(IDX_OWNER_UUID2, ownerUuid == null ? 0 : ownerUuid.getLeastSignificantBits())
                    + PbUtil.sizeOfInt64Field(IDX_TTL_MILLIS, ttlMillis)
                    + PbUtil.sizeOfInt32Field(IDX_KEYS_SIZE, keys == null ? 0 : keys.size())
                    + EncodeUtil.sizeOfBytesList(IDX_KEYS, keys)
                    + EncodeUtil.sizeOfBytesList(IDX_VALUES, values);
        }
        return encodeSize;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_GROUP_ID, groupId)) {
                    return false;
                }
                // fall through
            case IDX_GROUP_ID:
                if (!EncodeUtil.encode(context, destBuffer, IDX_KEY, key)) {
                    return false;
                }
                // fall through
            case IDX_KEY:
                if (!EncodeUtil.encode(context, destBuffer, IDX_VALUE, value)) {
                    return false;
                }
                // fall through
            case IDX_VALUE:
                if (!EncodeUtil.encode(context, destBuffer, IDX_EXPECT_VALUE, expectValue)) {
                    return false;
                }
            case IDX_EXPECT_VALUE:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_OWNER_UUID1,
                        ownerUuid == null ? 0 : ownerUuid.getMostSignificantBits())) {
                    return false;
                }
                // fall through
            case IDX_OWNER_UUID1:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_OWNER_UUID2,
                        ownerUuid == null ? 0 : ownerUuid.getLeastSignificantBits())) {
                    return false;
                }
                // fall through
            case IDX_OWNER_UUID2:
                if (!EncodeUtil.encodeInt64(context, destBuffer, IDX_TTL_MILLIS, ttlMillis)) {
                    return false;
                }
                // fall through
            case IDX_TTL_MILLIS:
                if (keys != null && !EncodeUtil.encodeInt32(context, destBuffer, IDX_KEYS_SIZE, keys.size())) {
                    return false;
                }
                // fall through
            case IDX_KEYS_SIZE:
                if (keys != null && !EncodeUtil.encodeBytesList(context, destBuffer, IDX_KEYS, keys)) {
                    return false;
                }
                // fall through
            case IDX_KEYS:
                return values == null || EncodeUtil.encodeBytesList(context, destBuffer, IDX_VALUES, values);
            default:
                throw new CodecException(context);
        }
    }
}
