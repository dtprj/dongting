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

import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.dtkv.KvResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static com.github.dtprj.dongting.dtkv.server.KvImpl.checkExistNode;
import static com.github.dtprj.dongting.dtkv.server.KvImplTest.ba;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
class KvCheckTest {

    private Timestamp ts;

    @BeforeEach
    void setUp() {
        ts = new Timestamp();
    }

    private KvImpl.OpContext createOpContext(int bizType, UUID operator, long ttlMillis) {
        KvImpl.OpContext ctx = new KvImpl.OpContext();
        ctx.init(bizType, operator, ttlMillis, ts.wallClockMillis, ts.nanoTime);
        return ctx;
    }

    private KvNodeHolder createKvNodeHolder(ByteArray key, boolean removed, TtlInfo ttlInfo) {
        KvNodeEx node;

        if (removed) {
            node = new KvNodeEx(1, ts.wallClockMillis, 1, ts.wallClockMillis);
        } else {
            // ttl manager does not distinguish between dir and data node, so use a simple data node for testing
            int flag = ttlInfo != null ? KvNode.FLAG_TEMP_MASK : 0;
            node = new KvNodeEx(1, ts.wallClockMillis, 1,
                    ts.wallClockMillis, flag, key.getData());
            node.ttlInfo = ttlInfo;
        }

        return new KvNodeHolder(ba("k1"), ba("k1"), node, null);
    }

    @Test
    void testCheckExistNodeForPut() {
        UUID owner = UUID.randomUUID();
        ByteArray key = ba("k1");

        KvImpl.OpContext ctx = createOpContext(DtKV.BIZ_TYPE_PUT, owner, 0);
        assertNull(checkExistNode(null, ctx));

        KvNodeHolder removedHolder = createKvNodeHolder(key, true, null);
        assertNull(checkExistNode(removedHolder, ctx));

        KvNodeHolder normalHolder = createKvNodeHolder(key, false, null);
        assertNull(checkExistNode(normalHolder, ctx));

        TtlInfo ttlInfo = new TtlInfo(ba("k1"), 1, owner, 1000, 5000,
                ts.nanoTime + 5000L, 1);
        KvNodeHolder tempHolder = createKvNodeHolder(key, false, ttlInfo);
        KvResult result = checkExistNode(tempHolder, ctx);
        assertNotNull(result);
        assertEquals(KvCodes.IS_TEMP_NODE, result.getBizCode());
    }

    @Test
    void testCheckExistNodeForMkTempDir() {
        UUID owner = UUID.randomUUID();
        UUID otherOwner = UUID.randomUUID();
        ByteArray key = ba("k1");

        KvImpl.OpContext ctx = createOpContext(DtKV.BIZ_MK_TEMP_DIR, owner, 0);

        // 1. not exist
        assertNull(checkExistNode(null, ctx));

        // 2. removed
        KvNodeHolder removedHolder = createKvNodeHolder(key, true, null);
        assertNull(checkExistNode(removedHolder, ctx));

        // 3. exists but not temp node
        KvNodeHolder normalHolder = createKvNodeHolder(key, false, null);
        KvResult result = checkExistNode(normalHolder, ctx);
        assertEquals(KvCodes.NOT_TEMP_NODE, result.getBizCode());

        // 4. exists and is temp node, success
        TtlInfo ttlInfo = new TtlInfo(ba("k1"), 1, owner, 1000, 5000, ts.nanoTime + 5000L, 1);
        KvNodeHolder tempHolder = createKvNodeHolder(key, false, ttlInfo);
        assertNull(checkExistNode(tempHolder, ctx));

        // 5. exists and is temp node, but not owner
        TtlInfo otherTtlInfo = new TtlInfo(ba("k1"), 1, otherOwner, 1000, 5000, ts.nanoTime + 5000L, 2);
        KvNodeHolder otherTempHolder = createKvNodeHolder(key, false, otherTtlInfo);
        result = checkExistNode(otherTempHolder, ctx);
        assertEquals(KvCodes.NOT_OWNER, result.getBizCode());
    }

    @Test
    void testCheckExistNodeForRemove() {
        UUID owner = UUID.randomUUID();
        UUID otherOwner = UUID.randomUUID();
        ByteArray key = ba("k1");

        KvImpl.OpContext ctx = createOpContext(DtKV.BIZ_TYPE_REMOVE, owner, 0);

        KvResult result = checkExistNode(null, ctx);
        assertEquals(KvCodes.NOT_FOUND, result.getBizCode());

        KvNodeHolder removedHolder = createKvNodeHolder(key, true, null);
        result = checkExistNode(removedHolder, ctx);
        assertEquals(KvCodes.NOT_FOUND, result.getBizCode());

        KvNodeHolder normalHolder = createKvNodeHolder(key, false, null);
        assertNull(checkExistNode(normalHolder, ctx));

        TtlInfo ttlInfo = new TtlInfo(ba("test"), 1, owner, 1000, 5000, ts.nanoTime + 5000L, 1);
        KvNodeHolder tempHolder = createKvNodeHolder(key, false, ttlInfo);
        assertNull(checkExistNode(tempHolder, ctx));

        TtlInfo otherTtlInfo = new TtlInfo(new ByteArray("test".getBytes()), 1, otherOwner, 1000, 5000, ts.nanoTime + 5000L, 2);
        KvNodeHolder otherTempHolder = createKvNodeHolder(key, false, otherTtlInfo);
        result = checkExistNode(otherTempHolder, ctx);
        assertEquals(KvCodes.NOT_OWNER, result.getBizCode());
    }

    @Test
    void testCheckExistNodeForUpdateTtl() {
        UUID owner = UUID.randomUUID();
        UUID otherOwner = UUID.randomUUID();
        ByteArray key = ba("k1");

        KvImpl.OpContext ctx = createOpContext(DtKV.BIZ_TYPE_UPDATE_TTL, owner, 0);

        KvResult result = checkExistNode(null, ctx);
        assertNotNull(result);
        assertEquals(KvCodes.NOT_FOUND, result.getBizCode());

        KvNodeHolder normalHolder = createKvNodeHolder(key, false, null);
        result = checkExistNode(normalHolder, ctx);
        assertNotNull(result);
        assertEquals(KvCodes.NOT_TEMP_NODE, result.getBizCode());

        TtlInfo ttlInfo = new TtlInfo(new ByteArray("test".getBytes()), 1, owner, 1000, 5000, ts.nanoTime + 5000L, 1);
        KvNodeHolder tempHolder = createKvNodeHolder(key, false, ttlInfo);
        assertNull(checkExistNode(tempHolder, ctx));

        TtlInfo otherTtlInfo = new TtlInfo(new ByteArray("test".getBytes()), 1, otherOwner, 1000, 5000, ts.nanoTime + 5000L, 2);
        KvNodeHolder otherTempHolder = createKvNodeHolder(key, false, otherTtlInfo);
        result = checkExistNode(otherTempHolder, ctx);
        assertNotNull(result);
        assertEquals(KvCodes.NOT_OWNER, result.getBizCode());
    }

    @Test
    void testCheckExistNodeForExpire() {
        UUID owner = UUID.randomUUID();
        KvImpl.OpContext ctx = createOpContext(DtKV.BIZ_TYPE_EXPIRE, owner, 0);
        assertThrows(IllegalStateException.class, () -> checkExistNode(null, ctx));
    }

}
