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
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class DtKVTest extends BaseFiberTest {
    private DtKV dtKV;
    int ver;

    @BeforeEach
    void setUp() {
        ver = 1;
        dtKV = createAndStart();
    }

    private DtKV createAndStart() {
        RaftGroupConfigEx groupConfig = new RaftGroupConfigEx(0, "1", "");
        groupConfig.setFiberGroup(fiberGroup);
        groupConfig.setTs(fiberGroup.getDispatcher().getTs());
        KvConfig kvConfig = new KvConfig();
        kvConfig.setUseSeparateExecutor(false);
        kvConfig.setInitMapCapacity(16);
        dtKV = new DtKV(groupConfig, kvConfig);
        dtKV.start();
        return dtKV;
    }

    @AfterEach
    void tearDown() {
        dtKV.stop(new DtTime(1, TimeUnit.SECONDS));
    }

    private KvResult put(int index, String key, String value) {
        RaftInput i = new RaftInput(DtKV.BIZ_TYPE_PUT, new ByteArray(key.getBytes()),
                new ByteArray(value.getBytes()), new DtTime(1, TimeUnit.SECONDS), false);
        FiberFuture<Object> f = dtKV.exec(index, i);
        assertTrue(f.isDone());
        return (KvResult) f.getResult();
    }

    private KvResult remove(int index, String key) {
        RaftInput i = new RaftInput(DtKV.BIZ_TYPE_REMOVE, new ByteArray(key.getBytes()),
                null, new DtTime(1, TimeUnit.SECONDS), false);
        FiberFuture<Object> f = dtKV.exec(index, i);
        assertTrue(f.isDone());
        return (KvResult) f.getResult();
    }

    private KvResult mkdir(int index, String key) {
        RaftInput i = new RaftInput(DtKV.BIZ_TYPE_MKDIR, new ByteArray(key.getBytes()),
                null, new DtTime(1, TimeUnit.SECONDS), false);
        FiberFuture<Object> f = dtKV.exec(index, i);
        assertTrue(f.isDone());
        return (KvResult) f.getResult();
    }

    private KvResult get(String key) {
        return dtKV.get(new ByteArray(key.getBytes()));
    }

    private Pair<Integer, List<KvResult>> list(String key) {
        return dtKV.list(new ByteArray(key.getBytes()));
    }

    @Test
    void simpleTest() throws Exception {
        doInFiber(() -> {
            assertEquals(KvCodes.CODE_SUCCESS, mkdir(ver++, "parent").getBizCode());
            assertEquals(KvCodes.CODE_SUCCESS, put(ver++, "parent.child1", "v1").getBizCode());
            assertEquals(KvCodes.CODE_SUCCESS, get("parent.child1").getBizCode());
            assertEquals("v1", new String(get("parent.child1").getNode().getData()));
            assertEquals(KvCodes.CODE_SUCCESS, remove(ver++, "parent.child1").getBizCode());
            assertEquals(KvCodes.CODE_SUCCESS, list("").getLeft());
            assertEquals(1, list("").getRight().size());
        });
    }
}
