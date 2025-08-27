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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.dtkv.KvResp;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.raft.test.TestUtil;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.dtprj.dongting.test.Tick.tick;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class DtKVServerTest extends ServerTestBase {
    @Test
    void test() throws Exception {
        ServerInfo s1 = null;
        KvClient client = new KvClient();

        try {
            s1 = createServer(1, "1, 127.0.0.1:4001", "1", "");
            waitStart(s1);

            client.start();
            client.getRaftClient().clientAddNode("1, 127.0.0.1:5001");
            client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1});

            testSimple(client);
            testTtl(client);

        } finally {
            TestUtil.stop(client);
            waitStop(s1);
        }
    }

    private void testSimple(KvClient client) {
        client.mkdir(groupId, "dir1".getBytes());
        client.put(groupId, "dir1.k1".getBytes(), "v1".getBytes());

        KvNode result = client.get(groupId, "dir1.k1".getBytes());
        assertEquals("v1", new String(result.data));

        List<KvResult> listResult = client.list(groupId, "".getBytes());
        assertEquals(groupId, listResult.size());
        assertEquals(KvCodes.SUCCESS, listResult.get(0).getBizCode());
        assertEquals("dir1", listResult.get(0).getKeyInDir().toString());

        client.remove(groupId, "dir1.k1".getBytes());
        result = client.get(groupId, "dir1.k1".getBytes());
        assertNull(result);

        // Test batchPut
        KvResp batchPutResults = client.batchPut(groupId, Arrays.asList("batchK1".getBytes(), "batchK2".getBytes()),
                Arrays.asList("v1".getBytes(), "v2".getBytes()));
        assertEquals(2, batchPutResults.results.size());
        assertEquals(KvCodes.SUCCESS, batchPutResults.results.get(0).getBizCode());
        assertEquals(KvCodes.SUCCESS, batchPutResults.results.get(1).getBizCode());

        // Verify batchPut results
        List<KvNode> batchGetResults = client.batchGet(groupId, Arrays.asList(
                "batchK1".getBytes(), "batchK2".getBytes()));
        assertEquals(2, batchGetResults.size());
        assertEquals("v1", new String(batchGetResults.get(0).data));
        assertEquals("v2", new String(batchGetResults.get(1).data));

        // Test batchRemove
        KvResp batchRemoveResults = client.batchRemove(groupId, Arrays.asList(
                "batchK1".getBytes(), "batchK2".getBytes()));
        assertEquals(2, batchRemoveResults.results.size());
        assertEquals(KvCodes.SUCCESS, batchRemoveResults.results.get(0).getBizCode());
        assertEquals(KvCodes.SUCCESS, batchRemoveResults.results.get(1).getBizCode());

        // Verify batchRemove results
        batchGetResults = client.batchGet(groupId, Arrays.asList(
                "batchK1".getBytes(), "batchK2".getBytes()));
        assertEquals(2, batchGetResults.size());
        assertNull(batchGetResults.get(0));
        assertNull(batchGetResults.get(1));

        // Test compareAndSet
        boolean casResult = client.compareAndSet(groupId, "casKey1".getBytes(), null, "value1".getBytes());
        assertTrue(casResult);
        assertEquals("value1", new String(client.get(groupId, "casKey1".getBytes()).data));
    }

    private void testTtl(KvClient client) throws Exception {
        CountDownLatch latch = new CountDownLatch(4);
        AtomicReference<Throwable> exRef = new AtomicReference<>();
        FutureCallback<Long> callback = (r, e) -> {
            if (e != null) {
                exRef.compareAndSet(null, e);
            }
            latch.countDown();
        };
        long ttlMillis = 10;
        client.makeTempDir(groupId, "tempDir1".getBytes(), tick(ttlMillis), callback);
        client.putTemp(groupId, "tempKey1".getBytes(), "tempValue1".getBytes(), tick(ttlMillis), callback);
        client.putTemp(groupId, "tempKey2".getBytes(), "tempValue2".getBytes(), tick(ttlMillis), callback);
        client.put(groupId, "tempDir1.k1".getBytes(), "tempValue2".getBytes(), callback);
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertNull(exRef.get());

        client.updateTtl(groupId, "tempKey1".getBytes(), 100000);

        WaitUtil.waitUtil(null, () -> client.get(groupId, "tempDir1".getBytes()));
        WaitUtil.waitUtil(null, () -> client.get(groupId, "tempKey2".getBytes()));

        assertNull(client.get(groupId, "tempDir1.k1".getBytes()));
        assertNotNull(client.get(groupId, "tempKey1".getBytes()));
    }
}
