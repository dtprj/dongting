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
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
public class MultiRaftTest extends ServerTestBase {

    @Test
    void test() throws Exception {
        AdminRaftClient adminClient = new AdminRaftClient();
        KvClient client = new KvClient();

        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002";
        String members = "1,2";
        ServerInfo s1 = null;
        ServerInfo s2 = null;

        try {
            s1 = createServer(1, servers, members, "");
            s2 = createServer(2, servers, members, "");
            waitStart(s1);
            waitStart(s2);

            int groupId2 = groupId + 1;

            adminClient.start();
            adminClient.clientAddNode(servers);

            DtTime timeout = new DtTime(10, TimeUnit.SECONDS);
            CompletableFuture<Void> f1 = adminClient.serverAddGroup(1, groupId2, members, "", timeout);
            CompletableFuture<Void> f2 = adminClient.serverAddGroup(2, groupId2, members, "", timeout);
            f1.get(5, TimeUnit.SECONDS);
            f2.get(5, TimeUnit.SECONDS);

            waitLeaderElectAndGetLeaderId(groupId2, s1, s2);

            client.start();
            client.getRaftClient().clientAddNode("1,127.0.0.1:5001;2,127.0.0.1:5002");
            client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1, 2});
            client.getRaftClient().clientAddOrUpdateGroup(groupId2, new int[]{1, 2});
            client.put(groupId, "key".getBytes(), "value1".getBytes());
            client.put(groupId2, "key".getBytes(), "value2".getBytes());
            KvNode n1 = client.get(groupId, "key".getBytes());
            KvNode n2 = client.get(groupId2, "key".getBytes());
            assertEquals("value1", new String(n1.data));
            assertEquals("value2", new String(n2.data));

            List<RaftNode> nodes = adminClient.serverListNodes(1).get(2, TimeUnit.SECONDS);
            assertEquals(2, nodes.size());
            assertEquals(1, nodes.get(0).nodeId);
            assertEquals(2, nodes.get(1).nodeId);

            int[] groupIds = adminClient.serverListGroups(1).get(2, TimeUnit.SECONDS);
            assertEquals(2, groupIds.length);
            assertEquals(groupId, groupIds[0]);
            assertEquals(groupId2, groupIds[1]);

            f1 = adminClient.serverRemoveGroup(1, groupId2, timeout);
            f2 = adminClient.serverRemoveGroup(2, groupId2, timeout);
            f1.get(5, TimeUnit.SECONDS);
            f2.get(5, TimeUnit.SECONDS);

            client.getRaftClient().clientRemoveGroup(groupId2);
        } finally {
            TestUtil.stop(adminClient);
            TestUtil.stop(client);
            waitStop(s1);
            waitStop(s2);
        }
    }
}

