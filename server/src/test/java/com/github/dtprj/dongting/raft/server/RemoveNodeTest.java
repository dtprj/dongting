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
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.RaftMember;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.test.TestUtil;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RemoveNodeTest extends ServerTestBase {

    @Test
    void testReplayLogsReferencingRemovedNode() throws Exception {
        AdminRaftClient adminClient = new AdminRaftClient();
        KvClient kvClient = new KvClient();
        ServerInfo s2 = null, s3 = null;
        try {
            String servers = "2,127.0.0.1:14402;3,127.0.0.1:14403";
            String members = "2,3";
            s2 = createServer(2, servers, members, "");
            s3 = createServer(3, servers, members, "");
            waitStart(s2);
            waitStart(s3);

            adminClient.start();
            adminClient.clientAddNode(servers);
            adminClient.clientAddOrUpdateGroup(groupId, new int[]{2, 3});
            adminClient.fetchLeader(groupId).get(2, TimeUnit.SECONDS);

            kvClient.start();
            kvClient.getRaftClient().clientAddNode("2,127.0.0.1:15502;3,127.0.0.1:15503");
            kvClient.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{2, 3});

            // data before all config changes, server 2 must replay it from the beginning of the log
            kvClient.put(groupId, "key1".getBytes(), "value1".getBytes());

            DtTime timeout = new DtTime(10, TimeUnit.SECONDS);

            // define node 4 on all servers, then prepare a config change which references it,
            // node 4 never starts
            adminClient.serverAddNode(2, 4, "127.0.0.1", 14404).get(5, TimeUnit.SECONDS);
            adminClient.serverAddNode(3, 4, "127.0.0.1", 14404).get(5, TimeUnit.SECONDS);
            adminClient.prepareChange(groupId, Set.of(2, 3), Set.of(),
                    Set.of(2, 3, 4), Set.of(), timeout).get(5, TimeUnit.SECONDS);

            // node 4 is referenced by the prepared raft members now
            assertRemoveNodeInUse(adminClient, 2, 4);
            assertRemoveNodeInUse(adminClient, 3, 4);

            // data between the config changes
            kvClient.put(groupId, "key2".getBytes(), "value2".getBytes());

            // abort the config change, the prepared members which reference node 4 are dropped
            long abortIndex = adminClient.abortChange(groupId, timeout).get(5, TimeUnit.SECONDS);
            ServerInfo finalS2 = s2;
            ServerInfo finalS3 = s3;
            WaitUtil.waitUtil(() -> finalS2.gc.raftStatus.getLastApplied() >= abortIndex,
                    finalS2.gc.fiberGroup.getExecutor());
            WaitUtil.waitUtil(() -> finalS3.gc.raftStatus.getLastApplied() >= abortIndex,
                    finalS3.gc.fiberGroup.getExecutor());

            // node 4 is not referenced by any raft member, the definition can be removed
            adminClient.serverRemoveNode(2, 4).get(5, TimeUnit.SECONDS);
            adminClient.serverRemoveNode(3, 4).get(5, TimeUnit.SECONDS);
            List<RaftNode> nodes = adminClient.serverListNodes(2).get(5, TimeUnit.SECONDS);
            assertEquals(Set.of(2, 3), nodes.stream().map(n -> n.nodeId).collect(Collectors.toSet()));

            // restart server 2. no snapshot exists, so it replays all raft logs from index 1.
            // the logs contain config changes which reference the removed node 4, but the node
            // definition does not exist any more. the group must start and replay normally.
            waitStop(s2);
            s2 = createServer(2, servers, members, "");
            waitStart(s2);
            ServerInfo restartedS2 = s2;
            WaitUtil.waitUtil(() -> restartedS2.gc.raftStatus.getLastApplied() >= abortIndex,
                    restartedS2.gc.fiberGroup.getExecutor());

            // the replay is from index 1: no snapshot is installed and no log is truncated
            assertEquals(1, restartedS2.gc.raftStatus.firstValidIndex);

            // query the restarted server directly: the config changes are all replayed,
            // no pending config change is left
            QueryStatusResp resp = adminClient.queryRaftServerStatus(2, groupId)
                    .get(5, TimeUnit.SECONDS);
            assertEquals(Set.of(2, 3), resp.members);
            assertTrue(resp.preparedMembers.isEmpty());
            assertTrue(resp.preparedObservers.isEmpty());

            // the group still serves data written before and between the config changes
            KvNode n1 = kvClient.get(groupId, "key1".getBytes());
            assertEquals("value1", new String(n1.data));
            KvNode n2 = kvClient.get(groupId, "key2".getBytes());
            assertEquals("value2", new String(n2.data));
        } finally {
            TestUtil.stop(adminClient);
            TestUtil.stop(kvClient);
            waitStop(s2);
            waitStop(s3);
        }
    }

    @Test
    void testMemberWithMissingNodeDefinition() throws Exception {
        AdminRaftClient adminClient = new AdminRaftClient();
        ServerInfo s2 = null, s3 = null;
        try {
            String servers = "2,127.0.0.1:14402;3,127.0.0.1:14403";
            String members = "2,3";
            s2 = createServer(2, servers, members, "");
            s3 = createServer(3, servers, members, "");
            waitStart(s2);
            waitStart(s3);
            ServerInfo leader = waitLeaderElectAndGetLeaderId(groupId, s2, s3);

            adminClient.start();
            adminClient.clientAddNode(servers);
            adminClient.clientAddOrUpdateGroup(groupId, new int[]{2, 3});
            adminClient.fetchLeader(groupId).get(2, TimeUnit.SECONDS);

            GroupComponents gc = leader.gc;
            Executor groupExecutor = gc.fiberGroup.getExecutor();

            // apply a config change which references an undefined node, like replaying a raft
            // log which was written before the node definition was removed
            assertTrue(gc.fiberGroup.fireFiber("testApplyConfigWithMissingNode",
                    gc.memberManager.applyConfigFrame("test apply config with missing node",
                            Set.of(2, 3, 99), Set.of(), Set.of(), Set.of())));
            WaitUtil.waitUtil(() -> findMember(gc, 99) != null, groupExecutor);

            // the member exists but its node is null; the leader role is not changed
            RaftMember m99 = findMember(gc, 99);
            assertNull(m99.node);
            assertEquals(RaftRole.leader, gc.raftStatus.getRole());

            // after the node definition is added, the member resolves it automatically
            adminClient.serverAddNode(leader.nodeId, 99, "127.0.0.1", 15999)
                    .get(5, TimeUnit.SECONDS);
            WaitUtil.waitUtil(() -> m99.node != null, groupExecutor);

            // the resolved member holds a reference to the node
            assertRemoveNodeInUse(adminClient, leader.nodeId, 99);
        } finally {
            TestUtil.stop(adminClient);
            waitStop(s2);
            waitStop(s3);
        }
    }

    @Test
    void testRemoveNodeAfterGroupStop() throws Exception {
        AdminRaftClient adminClient = new AdminRaftClient();
        ServerInfo s2 = null, s3 = null;
        try {
            // node 4 is defined in the servers config and used as an observer, it never starts
            String servers = "2,127.0.0.1:14402;3,127.0.0.1:14403;4,127.0.0.1:14404";
            String members = "2,3";
            s2 = createServer(2, servers, members, "4");
            s3 = createServer(3, servers, members, "4");
            waitStart(s2);
            waitStart(s3);

            adminClient.start();
            adminClient.clientAddNode(servers);
            adminClient.clientAddOrUpdateGroup(groupId, new int[]{2, 3});
            adminClient.fetchLeader(groupId).get(2, TimeUnit.SECONDS);

            DtTime timeout = new DtTime(10, TimeUnit.SECONDS);

            // the running raft groups reference node 4
            assertRemoveNodeInUse(adminClient, 2, 4);
            assertRemoveNodeInUse(adminClient, 3, 4);

            // remove the group from server 2, all references on server 2 are released
            adminClient.serverRemoveGroup(2, groupId, timeout).get(5, TimeUnit.SECONDS);
            ServerInfo finalS2 = s2;
            WaitUtil.waitUtil(() -> finalS2.raftServer.getRaftGroup(groupId) == null);

            // the node definition can be removed on server 2 now
            adminClient.serverRemoveNode(2, 4).get(5, TimeUnit.SECONDS);
            List<RaftNode> nodes = adminClient.serverListNodes(2).get(5, TimeUnit.SECONDS);
            assertEquals(Set.of(2, 3), nodes.stream().map(n -> n.nodeId).collect(Collectors.toSet()));

            // but the group on server 3 still references node 4
            assertRemoveNodeInUse(adminClient, 3, 4);
        } finally {
            TestUtil.stop(adminClient);
            waitStop(s2);
            waitStop(s3);
        }
    }

    private static RaftMember findMember(GroupComponents gc, int nodeId) {
        for (RaftMember m : gc.raftStatus.members) {
            if (m.nodeId == nodeId) {
                return m;
            }
        }
        return null;
    }

    private static void assertRemoveNodeInUse(AdminRaftClient adminClient,
                                              int nodeIdToInvoke, int nodeIdToRemove) throws Exception {
        CompletableFuture<Void> f = adminClient.serverRemoveNode(nodeIdToInvoke, nodeIdToRemove);
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> f.get(5, TimeUnit.SECONDS));
        boolean found = false;
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains("in use")) {
                found = true;
                break;
            }
        }
        assertTrue(found, "expect 'node is in use' error, but: " + e);
    }
}
