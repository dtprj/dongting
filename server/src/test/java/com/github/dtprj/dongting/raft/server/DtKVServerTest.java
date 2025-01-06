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

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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

        KvClient client = new KvClient(1, "1, 127.0.0.1:5001");
        client.start();
        client.mkdir(1, "dir1", timeout).get();
        client.put(1, "dir1.k1", "v1".getBytes(), timeout).get(1, TimeUnit.SECONDS);

        KvNode result = client.get(1, "dir1.k1", timeout).get(1, TimeUnit.SECONDS);
        assertEquals("v1", new String(result.getData()));

        List<KvResult> listResult = client.list(1, "", timeout).get(1, TimeUnit.SECONDS);
        assertEquals(1, listResult.size());
        assertEquals(KvCodes.CODE_SUCCESS, listResult.get(0).getBizCode());
        assertEquals("dir1", new String(listResult.get(0).getNode().getData()));

        client.remove(1, "dir1.k1", timeout).get(1, TimeUnit.SECONDS);
        result = client.get(1, "dir1.k1", timeout).get(1, TimeUnit.SECONDS);
        assertNull(result);

        client.stop(timeout);
        waitStop(s1);

    }
}
