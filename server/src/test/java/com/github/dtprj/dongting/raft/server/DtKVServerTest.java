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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.dtkv.KvResult;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class DtKVServerTest extends ServerTestBase {
    @Test
    void test() throws Exception {
        servicePortBase = 5000;
        ServerInfo s1 = createServer(1, "1, 127.0.0.1:4001", "1", "");

        waitStart(s1);
        DtTime timeout = new DtTime(5, TimeUnit.SECONDS);

        KvClient client = new KvClient();
        client.start();
        client.getRaftClient().clientAddNode("1, 127.0.0.1:5001");
        client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1});
        client.mkdir(groupId, "dir1".getBytes(), timeout);
        client.put(groupId, "dir1.k1".getBytes(), "v1".getBytes(), timeout);

        KvNode result = client.get(groupId, "dir1.k1".getBytes(), timeout);
        assertEquals("v1", new String(result.getData()));

        List<KvResult> listResult = client.list(groupId, "".getBytes(), timeout);
        assertEquals(groupId, listResult.size());
        assertEquals(KvCodes.CODE_SUCCESS, listResult.get(0).getBizCode());
        assertEquals("dir1", listResult.get(0).getKeyInDir().toString());

        client.remove(groupId, "dir1.k1".getBytes(), timeout);
        result = client.get(groupId, "dir1.k1".getBytes(), timeout);
        assertNull(result);

        // Test batchPut
        int[] batchPutResults = client.batchPut(groupId, Arrays.asList("batchK1".getBytes(), "batchK2".getBytes()),
                Arrays.asList("v1".getBytes(), "v2".getBytes()), timeout);
        assertEquals(2, batchPutResults.length);
        assertEquals(KvCodes.CODE_SUCCESS, batchPutResults[0]);
        assertEquals(KvCodes.CODE_SUCCESS, batchPutResults[1]);

        // Verify batchPut results
        List<KvNode> batchGetResults = client.batchGet(groupId, Arrays.asList(
                "batchK1".getBytes(), "batchK2".getBytes()), timeout);
        assertEquals(2, batchGetResults.size());
        assertEquals("v1", new String(batchGetResults.get(0).getData()));
        assertEquals("v2", new String(batchGetResults.get(1).getData()));

        // Test batchRemove
        int[] batchRemoveResults = client.batchRemove(groupId, Arrays.asList(
                "batchK1".getBytes(), "batchK2".getBytes()), timeout);
        assertEquals(2, batchRemoveResults.length);
        assertEquals(KvCodes.CODE_SUCCESS, batchRemoveResults[0]);
        assertEquals(KvCodes.CODE_SUCCESS, batchRemoveResults[1]);

        // Verify batchRemove results
        batchGetResults = client.batchGet(groupId, Arrays.asList(
                "batchK1".getBytes(), "batchK2".getBytes()), timeout);
        assertEquals(2, batchGetResults.size());
        assertNull(batchGetResults.get(0));
        assertNull(batchGetResults.get(1));

        // Test compareAndSet
        boolean casResult = client.compareAndSet(groupId, "casKey1".getBytes(), null, "value1".getBytes(), timeout);
        assertTrue(casResult);
        assertEquals("value1", new String(client.get(groupId, "casKey1".getBytes(), timeout).getData()));

        client.stop(timeout);
        waitStop(s1);

    }
}
