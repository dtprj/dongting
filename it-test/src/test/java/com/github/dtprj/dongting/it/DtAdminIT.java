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
package com.github.dtprj.dongting.it;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dist.Bootstrap;
import com.github.dtprj.dongting.it.support.BootstrapProcessManager;
import com.github.dtprj.dongting.it.support.ClusterValidator;
import com.github.dtprj.dongting.it.support.ConfigFileGenerator;
import com.github.dtprj.dongting.it.support.ConfigFileGenerator.ProcessConfig;
import com.github.dtprj.dongting.it.support.DtAdminProcessManager;
import com.github.dtprj.dongting.it.support.DtAdminProcessManager.AdminResult;
import com.github.dtprj.dongting.it.support.ItUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.test.TestDir;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for DtAdmin command-line tool.
 * These tests verify the admin commands by starting a 4-node cluster
 * (3 members + 1 observer) and testing various admin operations.
 * <p>
 * All tests share the same server cluster for better performance.
 *
 * @author huangli
 */
public class DtAdminIT {
    private static final DtLog log = DtLogs.getLogger(DtAdminIT.class);

    private static final int GROUP_ID = 0;
    private static final int NEW_GROUP_ID = 1000;
    private static final int[] MEMBER_IDS = {1, 2, 3};
    private static final String MEMBER_IDS_STR = "1,2,3";
    private static final int[] OBSERVER_IDS = {4};
    private static final String OBSERVER_IDS_STR = "4";
    private static final int[] ALL_NODE_IDS = {1, 2, 3, 4};

    private static final long ELECT_TIMEOUT = 1500;
    private static final long RPC_TIMEOUT = 500;
    private static final long CONNECT_TIMEOUT = 500;
    private static final long HEARTBEAT_INTERVAL = 700;
    private static final long PING_INTERVAL = 700;

    private static BootstrapProcessManager processManager;
    private static ClusterValidator validator;
    private static String serversPropertiesPath;

    @BeforeAll
    static void setupCluster() throws Exception {
        File tempDir = TestDir.createTestDir(DtAdminIT.class.getSimpleName());
        Path tempDirPath = tempDir.toPath();

        log.info("=== Setting up DtAdminIT test cluster ===");
        log.info("Temp directory: {}", tempDir);

        processManager = new BootstrapProcessManager();

        List<ProcessConfig> configs = new ConfigFileGenerator.ClusterConfigBuilder(MEMBER_IDS, GROUP_ID, tempDirPath)
                .observerIds(OBSERVER_IDS)
                .electTimeout(ELECT_TIMEOUT)
                .rpcTimeout(RPC_TIMEOUT)
                .connectTimeout(CONNECT_TIMEOUT)
                .heartbeatInterval(HEARTBEAT_INTERVAL)
                .pingInterval(PING_INTERVAL)
                .build();

        for (ProcessConfig config : configs) {
            processManager.startNode(config);
            log.info("Node {} started successfully", config.nodeId);
        }

        File dtAdminServersFile = new File(tempDir, "dt-admin-servers.properties");
        Properties props = new Properties();
        props.setProperty("servers", ItUtil.formatReplicateServers(ALL_NODE_IDS));
        props.setProperty(Bootstrap.GROUP_PREFIX + GROUP_ID + ".nodeIdOfMembers", MEMBER_IDS_STR);
        props.setProperty(Bootstrap.GROUP_PREFIX + GROUP_ID + ".nodeIdOfObservers", OBSERVER_IDS_STR);
        ConfigFileGenerator.writeConfigFile(props, dtAdminServersFile);
        serversPropertiesPath = dtAdminServersFile.getAbsolutePath();

        validator = new ClusterValidator();
        validator.initialize(ALL_NODE_IDS, GROUP_ID);
        int leaderId = validator.waitForClusterConsistency(GROUP_ID, ALL_NODE_IDS, 30);
        log.info("Leader elected: {}", leaderId);

        log.info("=== Cluster setup complete ===");
    }

    @AfterAll
    static void cleanupCluster() {
        log.info("=== Cleaning up DtAdminIT test cluster ===");

        if (validator != null) {
            try {
                validator.close();
            } catch (Exception e) {
                log.warn("Error closing validator", e);
            }
        }

        if (processManager != null) {
            processManager.stopAllNodes();
        }

        log.info("=== Cluster cleanup complete ===");
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testDtAdmin() throws Exception {
        testListNodesAndGroups();
        int leader = testTransferLeader();
        int[] newMembers = leader == 1 ? new int[]{1, 2} : new int[]{2, 3};
        testPrepareAndCommitChange(GROUP_ID, leader, newMembers, new int[]{});
        changeGroupConfig(GROUP_ID, leader, MEMBER_IDS, OBSERVER_IDS); // change back
        testAbortChange();
        testAddRemoveGroup();
        testRemoveNodeThenReAdd(leader);
    }

    private void testListNodesAndGroups() throws Exception {
        log.info("=== Running testListNodesAndGroups ===");

        int queryNodeId = ALL_NODE_IDS[new Random().nextInt(ALL_NODE_IDS.length)];

        log.info("Using node {} for queries", queryNodeId);

        AdminResult result = DtAdminProcessManager.executeAndVerify(
                serversPropertiesPath, "list-nodes", "--node-id", String.valueOf(queryNodeId));

        // Check if original nodes are visible
        long visibleNodes = result.stdoutLines.stream()
                .filter(line -> line.contains("Node{nodeId="))
                .count();
        assertEquals(0, result.exitCode);
        assertEquals(4, visibleNodes, "Should have 4 nodes visible, found: " + visibleNodes);

        result = DtAdminProcessManager.executeAndVerify(
                serversPropertiesPath, "query-status", "--node-id",
                String.valueOf(queryNodeId), "--group-id", String.valueOf(GROUP_ID));
        assertEquals(0, result.exitCode);
        assertTrue(result.stdoutLines.stream().anyMatch(line -> line.contains("Group Ready")));

        log.info("=== testListNodesAndGroups passed ===");
    }

    private int testTransferLeader() throws Exception {
        log.info("=== Running testTransferLeader ===");

        int originalLeaderId = getLeaderId();
        int newLeaderId = (originalLeaderId == 1) ? 2 : 1;
        log.info("Transferring leader from {} to {}", originalLeaderId, newLeaderId);

        AdminResult result = DtAdminProcessManager.executeAndVerify(
                serversPropertiesPath, "transfer-leader",
                "--group-id", String.valueOf(GROUP_ID),
                "--old-leader", String.valueOf(originalLeaderId),
                "--new-leader", String.valueOf(newLeaderId));

        assertEquals(0, result.exitCode);
        assertTrue(result.stdoutLines.stream()
                .anyMatch(line -> line.contains("Transfer leader completed successfully")));

        validator.waitForClusterConsistency(GROUP_ID, ALL_NODE_IDS, 10);

        for (int nodeId : ALL_NODE_IDS) {
            WaitUtil.waitUtil(() -> {
                try {
                    CompletableFuture<QueryStatusResp> f = validator.getAdminClient().queryRaftServerStatus(nodeId, GROUP_ID);
                    QueryStatusResp r = f.get(1, TimeUnit.SECONDS);
                    return r.leaderId == newLeaderId;
                } catch (Exception e) {
                    log.error("Error getting query status for node {}", nodeId, e);
                    return false;
                }
            });
        }

        log.info("=== testTransferLeader passed ===");
        return newLeaderId;
    }

    private void testPrepareAndCommitChange(int groupId, int leader, int[] newMembers, int[] newObservers) throws Exception {
        log.info("=== Running testPrepareAndCommitChange ===");

        changeGroupConfig(groupId, leader, newMembers, newObservers);

        log.info("=== testPrepareAndCommitChange passed ===");
    }

    private static void changeGroupConfig(int groupId, int leader, int[] newMembers, int[] newObservers) throws Exception {
        QueryStatusResp status = validator.getAdminClient().queryRaftServerStatus(leader, groupId).get(2, TimeUnit.SECONDS);

        String oldMembersStr = status.members.stream().map(String::valueOf).collect(Collectors.joining(","));
        String oldObserversStr = status.observers.stream().map(String::valueOf).collect(Collectors.joining(","));
        String newMembersStr = Arrays.stream(newMembers).mapToObj(String::valueOf).collect(Collectors.joining(","));
        String newObserversStr = Arrays.stream(newObservers).mapToObj(String::valueOf).collect(Collectors.joining(","));
        AdminResult prepareResult = DtAdminProcessManager.executeAndVerify(
                serversPropertiesPath, "prepare-change",
                "--group-id", String.valueOf(groupId),
                "--old-members", oldMembersStr,
                "--old-observers", oldObserversStr,
                "--new-members", newMembersStr,
                "--new-observers", newObserversStr);

        assertEquals(0, prepareResult.exitCode);
        String prepareIndexLine = prepareResult.stdoutLines.stream()
                .filter(line -> line.contains("Prepare index:"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Prepare index not found"));
        long prepareIndex = Long.parseLong(prepareIndexLine.split(":")[1].trim());
        log.info("Prepare index: {}", prepareIndex);

        AdminResult commitResult = DtAdminProcessManager.executeAndVerify(
                serversPropertiesPath, "commit-change",
                "--group-id", String.valueOf(GROUP_ID),
                "--prepare-index", String.valueOf(prepareIndex));

        assertEquals(0, commitResult.exitCode);
        assertTrue(commitResult.stdoutLines.stream().anyMatch(line -> line.contains("Commit index:")));

        int[] nodes = IntStream.concat(Arrays.stream(newMembers), Arrays.stream(newObservers)).toArray();
        validator.waitForClusterConsistency(groupId, nodes, 5);
    }

    private void testAbortChange() throws Exception {
        log.info("=== Running testAbortChange ===");

        AdminResult prepareResult = DtAdminProcessManager.executeAndVerify(
                serversPropertiesPath, "prepare-change",
                "--group-id", String.valueOf(GROUP_ID),
                "--old-members", "1,2,3",
                "--old-observers", "4",
                "--new-members", "2,3,4",
                "--new-observers", "");
        assertEquals(0, prepareResult.exitCode);

        AdminResult abortResult = DtAdminProcessManager.executeAndVerify(
                serversPropertiesPath, "abort-change", "--group-id", "0");

        assertEquals(0, abortResult.exitCode);
        assertTrue(abortResult.stdoutLines.stream().anyMatch(line -> line.contains("Abort index:")));

        validator.waitForClusterConsistency(GROUP_ID, ALL_NODE_IDS, 30);
        Map<Integer, QueryStatusResp> statusMap = validator.queryAllNodeStatus(GROUP_ID, ALL_NODE_IDS);
        QueryStatusResp status = statusMap.get(statusMap.values().iterator().next().leaderId);

        assertEquals(3, status.members.size());
        assertTrue(status.members.contains(1));
        assertTrue(status.members.contains(2));
        assertTrue(status.members.contains(3));
        assertTrue(status.observers.contains(4));

        log.info("=== testAbortChange passed ===");
    }

    private void testAddRemoveGroup() throws Exception {
        log.info("=== Running testAddGroup ===");

        int[] newGroupNodes = new int[]{1, 2};
        String membersStr = "1,2";
        for (int i : newGroupNodes) {
            AdminResult result = DtAdminProcessManager.executeAndVerify(
                    serversPropertiesPath, "add-group",
                    "--node-id", String.valueOf(i),
                    "--group-id", String.valueOf(NEW_GROUP_ID),
                    "--members", membersStr);
            assertEquals(0, result.exitCode);
            assertTrue(result.stdoutLines.stream()
                    .anyMatch(line -> line.contains("Add group completed successfully")));
        }
        validator.waitForClusterConsistency(NEW_GROUP_ID, newGroupNodes, 10);
        log.info("=== testAddGroup passed ===");

        log.info("=== Running testRemoveGroup ===");

        for (int i : newGroupNodes) {
            AdminResult result = DtAdminProcessManager.executeAndVerify(
                    serversPropertiesPath, "remove-group",
                    "--node-id", String.valueOf(i),
                    "--group-id", String.valueOf(NEW_GROUP_ID));
            assertEquals(0, result.exitCode);
            assertTrue(result.stdoutLines.stream()
                    .anyMatch(line -> line.contains("Remove group completed successfully")));
        }
        for (int i : newGroupNodes) {
            WaitUtil.waitUtil(() -> {
                try {
                    int[] groupIds = validator.getAdminClient().serverListGroups(i).get(2, TimeUnit.SECONDS);
                    return groupIds.length == 1 && GROUP_ID == groupIds[0];
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        log.info("=== testRemoveGroup passed ===");
    }

    private void testRemoveNodeThenReAdd(int leader) throws Exception {
        log.info("=== Running testRemoveNode ===");

        String newMembersStr;
        int[] newMembers;
        int[] nodeIdToRemove;
        if (leader == 1) {
            newMembers = new int[]{1, 2};
            newMembersStr = "1,2";
            nodeIdToRemove = new int[]{3, 4};
        } else {
            newMembers = new int[]{2, 3};
            newMembersStr = "2,3";
            nodeIdToRemove = new int[]{1, 4};
        }
        changeGroupConfig(GROUP_ID, leader, newMembers, new int[0]);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int nodeId : nodeIdToRemove) {
            CompletableFuture<Void> f = validator.getAdminClient().serverRemoveGroup(nodeId, GROUP_ID,
                    new DtTime(3, TimeUnit.SECONDS));
            futures.add(f);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(3, TimeUnit.SECONDS);
        for (int nodeId : newMembers) {
            for (int rid : nodeIdToRemove) {
                log.info("Removing node {} on {}", rid, nodeId);
                AdminResult result = DtAdminProcessManager.executeAndVerify(
                        serversPropertiesPath, "remove-node",
                        "--node-id", String.valueOf(nodeId),
                        "--remove-node-id", String.valueOf(rid));
                assertEquals(0, result.exitCode);
                assertTrue(result.stdoutLines.stream()
                        .anyMatch(line -> line.contains("Remove node completed successfully")));
            }
        }

        futures.clear();
        for (int nodeId : nodeIdToRemove) {
            CompletableFuture<Void> f = validator.getAdminClient().serverAddGroup(nodeId, GROUP_ID,
                    newMembersStr, "", new DtTime(3, TimeUnit.SECONDS));
            futures.add(f);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(3, TimeUnit.SECONDS);

        for (int nodeId : newMembers) {
            for (int rid : nodeIdToRemove) {
                log.info("Adding node {} on {}", rid, nodeId);
                AdminResult result = DtAdminProcessManager.executeAndVerify(
                        serversPropertiesPath, "add-node",
                        "--node-id", String.valueOf(nodeId),
                        "--add-node-id", String.valueOf(rid),
                        "--group-id", String.valueOf(GROUP_ID),
                        "--host", "127.0.0.1",
                        "--port", String.valueOf(ItUtil.replicatePort(rid))
                );
                assertEquals(0, result.exitCode);
                assertTrue(result.stdoutLines.stream()
                        .anyMatch(line -> line.contains("Add node completed successfully")));
            }
        }

        // all nodes become member
        changeGroupConfig(GROUP_ID, leader, ALL_NODE_IDS, new int[0]);

        validator.waitForClusterConsistency(GROUP_ID, ALL_NODE_IDS, 10);

        log.info("=== testRemoveNode passed ===");
    }

    private int getLeaderId() {
        Map<Integer, QueryStatusResp> status = validator.queryAllNodeStatus(GROUP_ID, ALL_NODE_IDS);
        for (Map.Entry<Integer, QueryStatusResp> entry : status.entrySet()) {
            if (entry.getValue().leaderId > 0) {
                return entry.getValue().leaderId;
            }
        }
        throw new RuntimeException("No leader found");
    }
}
