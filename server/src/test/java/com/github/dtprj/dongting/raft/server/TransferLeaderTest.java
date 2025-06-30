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
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
public class TransferLeaderTest extends ServerTestBase {

    @Test
    void test() throws Exception {
        ServerInfo[] sis = new ServerInfo[3];
        AdminRaftClient adminClient = new AdminRaftClient();
        KvClient client = new KvClient();
        try {
            String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003";
            String members = "1,2,3";
            String observers = "";
            sis[0] = createServer(1, servers, members, observers);
            sis[1] = createServer(2, servers, members, observers);
            sis[2] = createServer(3, servers, members, observers);
            for (ServerInfo si : sis) {
                waitStart(si);
            }

            ServerInfo leader = waitLeaderElectAndGetLeaderId(groupId, sis);

            client.start();
            client.getRaftClient().clientAddNode("1,127.0.0.1:5001;2,127.0.0.1:5002;3,127.0.0.1:5003");
            client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1, 2, 3});

            DtTime timeout = new DtTime(5, TimeUnit.SECONDS);
            client.put(groupId, "key".getBytes(), "value".getBytes());

            ServerInfo newLeader = leader == sis[0] ? sis[1] : sis[0];

            adminClient.start();
            adminClient.clientAddNode("1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003");
            adminClient.clientAddOrUpdateGroup(groupId, new int[]{1, 2, 3});
            adminClient.fetchLeader(groupId).get(2, TimeUnit.SECONDS);
            CompletableFuture<Void> f = adminClient.transferLeader(groupId, leader.nodeId, newLeader.nodeId, timeout);

            f.get(5, TimeUnit.SECONDS);

            assertEquals(RaftRole.follower, leader.group.groupComponents.raftStatus.getShareStatus().role);
            assertEquals(RaftRole.leader, newLeader.group.groupComponents.raftStatus.getShareStatus().role);

            client.put(groupId, "key".getBytes(), "value".getBytes());

        } finally {
            TestUtil.stop(adminClient);
            TestUtil.stop(client);
            for (ServerInfo si : sis) {
                waitStop(si);
            }
        }
    }
}
