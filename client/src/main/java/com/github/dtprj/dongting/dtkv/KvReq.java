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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.raft.RaftReq;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author huangli
 */
public class KvReq extends RaftReq implements Encodable {
    private static final int IDX_GROUP_ID = 1;
    private static final int IDX_KEY = 2;
    private static final int IDX_VALUE = 3;
    private static final int IDX_KEYS = 4;
    private static final int IDX_VALUES = 5;
    private static final int IDX_EXPECT_VALUE = 6;

    private String key;
    private RefBuffer value;
    private List<String> keys;
    private List<byte[]> values;
    private byte[] expectValue;

    public KvReq() {

    }

    @Override
    public int actualSize() {
        return 0;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        return false;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public RefBuffer getValue() {
        return value;
    }

    public void setValue(RefBuffer value) {
        this.value = value;
    }

    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public List<byte[]> getValues() {
        return values;
    }

    public void setValues(List<byte[]> values) {
        this.values = values;
    }

    public byte[] getExpectValue() {
        return expectValue;
    }

    public void setExpectValue(byte[] expectValue) {
        this.expectValue = expectValue;
    }
}
