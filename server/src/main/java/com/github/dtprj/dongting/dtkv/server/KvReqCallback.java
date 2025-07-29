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
import java.util.UUID;

/**
 * @author huangli
 */
// re-used
public class KvReqCallback extends PbCallback<KvReq> {

    private int keysSize;
    private long uuid1;
    private long uuid2;
    KvReq req;

    @Override
    protected void begin(int len) {
        req = new KvReq();
    }

    @Override
    protected boolean end(boolean success) {
        req = null;
        keysSize = 0;
        uuid1 = 0;
        uuid2 = 0;
        return success;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        switch (index) {
            case KvReq.IDX_GROUP_ID:
                req.groupId = (int) value;
                break;
            case KvReq.IDX_TTL_MILLIS:
                req.ttlMillis = value;
                break;
            case KvReq.IDX_KEYS_SIZE:
                keysSize = (int) value;
                break;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        switch (index) {
            case KvReq.IDX_KEY:
                req.key = parseBytes(buf, fieldLen, currentPos);
                break;
            case KvReq.IDX_VALUE:
                req.value = parseBytes(buf, fieldLen, currentPos);
                break;
            case KvReq.IDX_KEYS:
                if (req.keys == null) {
                    req.keys = createArrayList(keysSize);
                }
                byte[] k = parseBytes(buf, fieldLen, currentPos);
                if (k != null) {
                    req.keys.add(k);
                }
                break;
            case KvReq.IDX_VALUES:
                if (req.values == null) {
                    req.values = createArrayList(keysSize);
                }
                byte[] v = parseBytes(buf, fieldLen, currentPos);
                if (v != null) {
                    req.values.add(v);
                }
                break;
            case KvReq.IDX_EXPECT_VALUE:
                req.expectValue = parseBytes(buf, fieldLen, currentPos);
                break;
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        switch (index) {
            case KvReq.IDX_OWNER_UUID1:
                uuid1 = value;
                break;
            case KvReq.IDX_OWNER_UUID2:
                uuid2 = value;
                break;
        }
        return true;
    }

    @Override
    protected KvReq getResult() {
        req.checkKeysAndValues();
        if (uuid1 != 0 && uuid2 != 0) {
            req.ownerUuid = new UUID(uuid1, uuid2);
        }
        return req;
    }
}
