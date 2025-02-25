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
import java.util.ArrayList;

/**
 * @author huangli
 */
public class KvReq extends RaftRpcData implements Encodable {
    private static final int IDX_GROUP_ID = 1;
    private static final int IDX_KEY = 2;
    private static final int IDX_VALUE = 3;
    private static final int IDX_KEYS_SIZE = 4;
    private static final int IDX_KEYS = 5;
    private static final int IDX_VALUES_SIZE = 6;
    private static final int IDX_VALUES = 7;
    private static final int IDX_EXPECT_VALUE = 8;

    public final byte[] key;
    public final byte[] value;
    public final ArrayList<byte[]> keys;
    public final ArrayList<byte[]> values;
    public final byte[] expectValue;

    private int encodeSize;

    public KvReq(int groupId, byte[] key, byte[] value, ArrayList<byte[]> keys,
                 ArrayList<byte[]> values, byte[] expectValue) {
        this.groupId = groupId;
        this.key = key;
        this.value = value;
        this.keys = keys;
        this.values = values;
        this.expectValue = expectValue;
    }

    public KvReq(int groupId, byte[] key, byte[] value) {
        this.groupId = groupId;
        this.key = key;
        this.value = value;
        this.keys = null;
        this.values = null;
        this.expectValue = null;
    }

    public KvReq(int groupId, byte[] key, byte[] value, byte[] expectValue) {
        this.groupId = groupId;
        this.key = key;
        this.value = value;
        this.keys = null;
        this.values = null;
        this.expectValue = expectValue;
    }

    public KvReq(int groupId, ArrayList<byte[]> keys, ArrayList<byte[]> values) {
        this.groupId = groupId;
        this.key = null;
        this.value = null;
        this.keys = keys;
        this.values = values;
        this.expectValue = null;
    }

    @Override
    public int actualSize() {
        if (encodeSize == 0) {
            encodeSize = PbUtil.accurateUnsignedIntSize(IDX_GROUP_ID, groupId)
                    + EncodeUtil.actualSize(IDX_KEY, key)
                    + EncodeUtil.actualSize(IDX_VALUE, value)
                    + PbUtil.accurateUnsignedIntSize(IDX_KEYS_SIZE, keys == null ? 0 : keys.size())
                    + EncodeUtil.actualSizeOfBytes(IDX_KEYS, keys)
                    + PbUtil.accurateUnsignedIntSize(IDX_VALUES_SIZE, values == null ? 0 : values.size())
                    + EncodeUtil.actualSizeOfBytes(IDX_VALUES, values)
                    + EncodeUtil.actualSize(IDX_EXPECT_VALUE, expectValue);
        }
        return encodeSize;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeUint32(context, destBuffer, IDX_GROUP_ID, groupId)) {
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
                if (keys != null && !EncodeUtil.encodeUint32(context, destBuffer, IDX_KEYS_SIZE, keys.size())) {
                    return false;
                }
                // fall through
            case IDX_KEYS_SIZE:
                if (keys != null && !EncodeUtil.encodeBytes(context, destBuffer, IDX_KEYS, keys)) {
                    return false;
                }
                // fall through
            case IDX_KEYS:
                if (values != null && !EncodeUtil.encodeUint32(context, destBuffer, IDX_VALUES_SIZE, values.size())) {
                    return false;
                }
                // fall through
            case IDX_VALUES_SIZE:
                if (values != null && !EncodeUtil.encodeBytes(context, destBuffer, IDX_VALUES, values)) {
                    return false;
                }
                // fall through
            case IDX_VALUES:
                return expectValue == null || EncodeUtil.encode(context, destBuffer, IDX_EXPECT_VALUE, expectValue);
            default:
                throw new CodecException(context);
        }
    }
}
