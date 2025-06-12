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
    private static final int IDX_EXPECT_VALUE = 4;
    private static final int IDX_KEYS_SIZE = 5;
    private static final int IDX_KEYS = 6;
    private static final int IDX_VALUES = 7;

    int groupId;
    byte[] key;
    byte[] value;
    private int keysSize;
    ArrayList<byte[]> keys;
    ArrayList<byte[]> values;
    byte[] expectValue;

    @Override
    protected boolean end(boolean success) {
        groupId = 0;
        key = null;
        value = null;
        keysSize = 0;
        keys = null;
        values = null;
        expectValue = null;
        return success;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        if (index == IDX_GROUP_ID) {
            groupId = (int) value;
        } else if (index == IDX_KEYS_SIZE) {
            keysSize = (int) value;
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
                value = parseBytes(buf, fieldLen, currentPos);
                break;
            case IDX_KEYS:
                if (keys == null) {
                    keys = createArrayList(keysSize);
                }
                byte[] k = parseBytes(buf, fieldLen, currentPos);
                if (k != null) {
                    keys.add(k);
                }
                break;
            case IDX_VALUES:
                if (values == null) {
                    values = createArrayList(keysSize);
                }
                byte[] v = parseBytes(buf, fieldLen, currentPos);
                if (v != null) {
                    values.add(v);
                }
                break;
            case IDX_EXPECT_VALUE:
                expectValue = parseBytes(buf, fieldLen, currentPos);
                break;
        }
        return true;
    }

    @Override
    protected KvReq getResult() {
        return new KvReq(groupId, key, value, expectValue, keys, values);
    }
}
