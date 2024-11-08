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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.dtkv.KvReq;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author huangli
 */
// re-used
public class KvReqCallback extends PbCallback<KvReq> {

    private static final int IDX_GROUP_ID = 1;
    private static final int IDX_KEY = 2;
    private static final int IDX_VALUE = 3;
    private static final int IDX_KEYS_SIZE = 4;
    private static final int IDX_KEYS = 5;
    private static final int IDX_VALUES_SIZE = 6;
    private static final int IDX_VALUES = 7;
    private static final int IDX_EXPECT_VALUE = 8;

    int groupId;
    byte[] key;
    ByteArray value;
    private int keysSize;
    ArrayList<byte[]> keys;
    private int valuesSize;
    ArrayList<ByteArray> values;
    ByteArray expectValue;

    @Override
    public boolean readVarNumber(int index, long value) {
        if (index == IDX_GROUP_ID) {
            groupId = (int) value;
        } else if (index == IDX_KEYS_SIZE) {
            keysSize = (int) value;
        } else if (index == IDX_VALUES_SIZE) {
            valuesSize = (int) value;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        switch (index) {
            case IDX_KEY:
                key = parseBytes(buf, fieldLen, currentPos);
                break;
            case IDX_VALUE:
                value = parseByteArray(buf, fieldLen, currentPos);
                break;
            case IDX_KEYS:
                if (keys == null) {
                    keys = keysSize == 0 ? new ArrayList<>() : new ArrayList<>(keysSize);
                }
                byte[] k = parseBytes(buf, fieldLen, currentPos);
                if (k != null) {
                    keys.add(k);
                }
                break;
            case IDX_VALUES:
                if (values == null) {
                    values = valuesSize == 0 ? new ArrayList<>() : new ArrayList<>(valuesSize);
                }
                ByteArray v = parseByteArray(buf, fieldLen, currentPos);
                if (v != null) {
                    values.add(v);
                }
                break;
            case IDX_EXPECT_VALUE:
                expectValue = parseByteArray(buf, fieldLen, currentPos);
                break;
        }
        return true;
    }

    @Override
    protected KvReq getResult() {
        return new KvReq(groupId, key, value, keys, values, expectValue);
    }
}
