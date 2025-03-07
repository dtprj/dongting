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
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
public class MultiRaftTest extends ServerTestBase {

    @Test
    void test() throws Exception {
        servicePortBase = 5000;
        DtTime timeout = new DtTime(10, TimeUnit.SECONDS);
        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002";
        String members = "1,2";
        ServerInfo s1 = createServer(1, servers, members, "");
        ServerInfo s2 = createServer(2, servers, members, "");
        waitStart(s1);
        waitStart(s2);

        waitLeaderElectAndGetLeaderId(groupId, s1, s2);

        int groupId2 = groupId + 1;

        AdminRaftClient adminClient = new AdminRaftClient();
        adminClient.start();
        adminClient.clientAddNode(servers);

        CompletableFuture<Void> f1 = adminClient.serverAddGroup(1, groupId2, members, "", timeout);
        CompletableFuture<Void> f2 = adminClient.serverAddGroup(2, groupId2, members, "", timeout);
        f1.get(5, TimeUnit.SECONDS);
        f2.get(5, TimeUnit.SECONDS);

        waitLeaderElectAndGetLeaderId(groupId2, s1, s2);

        KvClient client = new KvClient();
        client.start();
        client.getRaftClient().clientAddNode("1,127.0.0.1:5001;2,127.0.0.1:5002");
        client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1, 2});
        client.getRaftClient().clientAddOrUpdateGroup(groupId2, new int[]{1, 2});
        client.put(groupId, "key".getBytes(), "value1".getBytes(), timeout);
        client.put(groupId2, "key".getBytes(), "value2".getBytes(), timeout);
        KvNode n1 = client.get(groupId, "key".getBytes(), timeout);
        KvNode n2 = client.get(groupId2, "key".getBytes(), timeout);
        assertEquals("value1", new String(n1.getData()));
        assertEquals("value2", new String(n2.getData()));

        f1 = adminClient.serverRemoveGroup(1, groupId2, timeout);
        f2 = adminClient.serverRemoveGroup(2, groupId2, timeout);
        f1.get(5, TimeUnit.SECONDS);
        f2.get(5, TimeUnit.SECONDS);

        adminClient.stop(timeout);
        client.stop(timeout);
        waitStop(s1);
        waitStop(s2);
    }
}

