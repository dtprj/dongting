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
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class ConfigChangeTest extends ServerTestBase {

    @Test
    void test() throws Exception {
        DtTime timeout = new DtTime(10, TimeUnit.SECONDS);
        String servers = "2,127.0.0.1:4002;3,127.0.0.1:4003";
        String members = "2,3";
        ServerInfo s2 = createServer(2, servers, members, "");
        ServerInfo s3 = createServer(3, servers, members, "");
        waitStart(s2);
        waitStart(s3);

        ServerInfo leader = waitLeaderElectAndGetLeaderId(s2, s3);

        RaftNode n4 = new RaftNode(4, new HostPort("127.0.0.1", 4004));
        s2.raftServer.addNode(n4, 1000);
        s3.raftServer.addNode(n4, 1000);

        ServerInfo follower = leader == s2 ? s3 : s2;

        AdminRaftClient c = new AdminRaftClient();
        c.start();
        c.addOrUpdateGroup(groupId, "2,127.0.0.1:4002;3,127.0.0.1:4003");
        c.fetchLeader(groupId).get(2, TimeUnit.SECONDS);

        CompletableFuture<Long> f = c.prepareConfigChange(groupId, Set.of(2, 3), Set.of(), Set.of(), Set.of(),
                Set.of(2, 3, 4), Set.of(), timeout);
        long prepareIndex = f.get(5, TimeUnit.SECONDS);

        put(leader, "k1", "v1"); // make follower apply the prepare operation as soon as possible
        // commit will check follower apply to prepareIndex
        TestUtil.waitUtil(() -> follower.group.getGroupComponents().getRaftStatus()
                .getShareStatus().lastApplied >= prepareIndex);

        f = c.commitChange(groupId, prepareIndex, timeout);
        f.get(5, TimeUnit.SECONDS);

        ServerInfo s4 = createServer(4, "2,127.0.0.1:4002;3,127.0.0.1:4003;4,127.0.0.1:4004", "2,3,4", "");
        waitStart(s4);

        assertTrue(() -> s4.group.getGroupComponents().getRaftStatus().getShareStatus().lastApplied >= prepareIndex);

        waitStop(s2);
        waitStop(s3);
        waitStop(s4);
    }
}
