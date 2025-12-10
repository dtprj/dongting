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
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.test.TestUtil;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class ReplicateTest extends ServerTestBase {

    @BeforeEach
    public void beforeEach() {
        this.idxCacheSize = 4;
        this.idxFlushThreshold = 2;
        this.idxItemsPerFile = 8;
        this.logFileSize = 1024;
        this.electTimeout = 50;
    }

    @Test
    void test() throws Exception {
        KvClient client = new KvClient();
        AdminRaftClient adminClient = new AdminRaftClient();
        ServerInfo s1 = null, s2 = null, s3 = null;
        try {
            String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003";
            String members = "1,2,3";
            String observers = "";
            s1 = createServer(1, servers, members, observers);
            s2 = createServer(2, servers, members, observers);

            waitStart(s1);
            waitStart(s2);

            ServerInfo leader = waitLeaderElectAndGetLeaderId(groupId, s1, s2);

            client.start();
            client.getRaftClient().clientAddNode("1,127.0.0.1:5001;2,127.0.0.1:5002;3,127.0.0.1:5003");
            client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1, 2, 3});

            HashMap<String, byte[]> expectMap = new HashMap<>();
            InstallTest.putValues(groupId, client, "beforeInstallKey",
                    10, 300, expectMap);

            // start server 3
            s3 = createServer(3, servers, members, observers);
            waitStart(s3);

            long raftIndex2 = InstallTest.putValues(groupId, client, "beforeInstallKey",
                    10, 300, expectMap);

            // wait for s3 to catch up
            RaftGroupImpl g3 = (RaftGroupImpl) s3.raftServer.getRaftGroup(1);
            WaitUtil.waitUtil(() -> g3.groupComponents.raftStatus.getLastApplied() >= raftIndex2,
                    g3.groupComponents.raftStatus.fiberGroup.getExecutor());

            DtTime timeout = new DtTime(5, TimeUnit.SECONDS);
            adminClient.start();
            adminClient.clientAddNode(servers);
            adminClient.clientAddOrUpdateGroup(groupId, new int[]{1, 2, 3});
            adminClient.transferLeader(groupId, leader.nodeId, 3, timeout).get(5, TimeUnit.SECONDS);

            InstallTest.check(groupId, client, expectMap);
        } finally {
            // stop all
            TestUtil.stop(adminClient);
            TestUtil.stop(client);
            waitStop(s1);
            waitStop(s2);
            waitStop(s3);
        }
    }

}
