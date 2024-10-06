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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author huangli
 */
public class KvReqCallback extends PbCallback<Void> {

    private static final int IDX_GROUP_ID = 1;
    private static final int IDX_KEY = 2;
    private static final int IDX_VALUE = 3;
    private static final int IDX_KEYS = 4;
    private static final int IDX_VALUES = 5;
    private static final int IDX_EXPECT_VALUE = 6;

    int groupId;
    String key;
    byte[] value;
    List<String> keys;
    List<byte[]> values;
    byte[] expectValue;

    @Override
    public boolean readVarNumber(int index, long value) {
        if (index == IDX_GROUP_ID) {
            groupId = (int) value;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        switch (index) {
            case IDX_KEY:
                key = parseUTF8(buf, fieldLen, currentPos);
                break;
            case IDX_VALUE:
                value = parseBytes(buf, fieldLen, currentPos);
                break;
            case IDX_KEYS:
                if (keys == null) {
                    keys = new ArrayList<>();
                }
                String k = parseUTF8(buf, fieldLen, currentPos);
                if (k != null) {
                    keys.add(k);
                }
                break;
            case IDX_VALUES:
                if (values == null) {
                    values = new ArrayList<>();
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
    protected Void getResult() {
        return null;
    }
}
