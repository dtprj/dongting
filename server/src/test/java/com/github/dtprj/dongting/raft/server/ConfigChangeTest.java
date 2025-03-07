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
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;
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

        waitLeaderElectAndGetLeaderId(groupId, s2, s3);

        AdminRaftClient c = new AdminRaftClient();
        c.start();
        c.clientAddNode(servers);
        c.clientAddOrUpdateGroup(groupId, new int[]{2, 3});
        c.fetchLeader(groupId).get(2, TimeUnit.SECONDS);

        CompletableFuture<Long> f = c.prepareConfigChange(groupId, Set.of(2, 3), Set.of(), Set.of(2), Set.of(), timeout);
        f.get(5, TimeUnit.SECONDS);

        f = c.abortChange(groupId, timeout);
        f.get(5, TimeUnit.SECONDS);

        CompletableFuture<Void> f1 = c.serverAddNode(2, 4, "127.0.0.1", 4004, timeout);
        CompletableFuture<Void> f2 = c.serverAddNode(3, 4, "127.0.0.1", 4004, timeout);
        f1.get(5, TimeUnit.SECONDS);
        f2.get(5, TimeUnit.SECONDS);

        f = c.prepareConfigChange(groupId, Set.of(2, 3), Set.of(), Set.of(2, 3, 4), Set.of(), timeout);
        long prepareIndex = f.get(5, TimeUnit.SECONDS);

        f = c.commitChange(groupId, prepareIndex, timeout);
        f.get(5, TimeUnit.SECONDS);

        ServerInfo s4 = createServer(4, "2,127.0.0.1:4002;3,127.0.0.1:4003;4,127.0.0.1:4004", "2,3,4", "");
        waitStart(s4);

        long finalPrepareIndex = prepareIndex;
        assertTrue(() -> s4.group.getGroupComponents().raftStatus.getShareStatus().lastApplied >= finalPrepareIndex);

        f = c.prepareConfigChange(groupId, Set.of(2, 3, 4), Set.of(), Set.of(2, 3), Set.of(), timeout);
        prepareIndex = f.get(5, TimeUnit.SECONDS);

        f = c.commitChange(groupId, prepareIndex, timeout);
        f.get(5, TimeUnit.SECONDS);

        f1 = c.serverRemoveNode(2, 4, timeout);
        f2 = c.serverRemoveNode(3, 4, timeout);
        f1.get(5, TimeUnit.SECONDS);
        f2.get(5, TimeUnit.SECONDS);

        waitStop(s2);
        waitStop(s3);
        waitStop(s4);
    }
}
