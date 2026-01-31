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
package com.github.dtprj.dongting.it.support;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tools for validating the correctness of the cluster.
 *
 * @author huangli
 */
@SuppressWarnings("BusyWait")
public class ClusterValidator {
    private static final Logger log = LoggerFactory.getLogger(ClusterValidator.class);

    private static final long QUERY_RETRY_INTERVAL_MS = 100;
    private static final long SINGLE_QUERY_TIMEOUT_SECONDS = 2;

    private final AdminRaftClient adminClient;

    public ClusterValidator() {
        adminClient = new AdminRaftClient();
    }

    /**
     * Initialize AdminRaftClient and configure cluster
     */
    public void initialize(int[] nodeIds, int groupId) {
        String servers = ItUtil.formatReplicateServers(nodeIds);

        adminClient.start();

        // Add node information
        adminClient.clientAddNode(servers);

        // Add group information
        adminClient.clientAddOrUpdateGroup(groupId, nodeIds);

        log.info("AdminRaftClient initialized successfully");
    }

    public AdminRaftClient getAdminClient() {
        return adminClient;
    }

    /**
     * Query status from all nodes
     */
    public Map<Integer, QueryStatusResp> queryAllNodeStatus(int groupId, int[] nodeIds) {
        Map<Integer, QueryStatusResp> result = new HashMap<>();

        for (int nodeId : nodeIds) {
            try {
                QueryStatusResp status = adminClient.queryRaftServerStatus(nodeId, groupId)
                        .get(SINGLE_QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                result.put(nodeId, status);
                log.debug("Queried status from node {}: term={}, leaderId={}",
                        nodeId, status.term, status.leaderId);
            } catch (Exception e) {
                log.debug("Failed to query node {}: {}", nodeId, e.getMessage());
                // Continue to query other nodes
            }
        }

        return result;
    }

    /**
     * Wait for all nodes in the cluster to reach full consistency
     * (same leader, term, members, observers, and groupReady flag)
     */
    public int waitForClusterConsistency(int groupId, int[] nodeIds, long timeoutSeconds) throws Exception {
        log.info("Waiting for cluster consistency (timeout: {} seconds)", timeoutSeconds);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        int attemptCount = 0;

        Map<Integer, QueryStatusResp> allStatus = null;
        while (System.nanoTime() - deadline < 0) {
            attemptCount++;

            try {
                allStatus = queryAllNodeStatus(groupId, nodeIds);
                if (allStatus != null && validateFullConsistency(allStatus) && allStatus.size() == nodeIds.length) {
                    QueryStatusResp sample = allStatus.values().iterator().next();
                    log.info("Cluster consistency achieved: leaderId={}, term={}, members={}",
                            sample.leaderId, sample.term, sample.members);
                    return allStatus.values().iterator().next().leaderId;
                } else {
                    if (attemptCount % 100 == 0) {
                        log.info("Cluster consistency validation failed, attempt {}", attemptCount);
                        printClusterStatus(allStatus);
                    }
                }
            } catch (Exception e) {
                log.info("Failed to query status (attempt {}): {}", attemptCount, e.getMessage());
            }
            Thread.sleep(QUERY_RETRY_INTERVAL_MS);
        }

        log.error("Cluster consistency timeout after {} attempts.", attemptCount);
        printClusterStatus(allStatus);

        throw new RuntimeException("Cluster consistency timeout after " + timeoutSeconds + " seconds");
    }

    private static void printClusterStatus(Map<Integer, QueryStatusResp> allStatus) {
        if (allStatus != null) {
            for (Map.Entry<Integer, QueryStatusResp> entry : allStatus.entrySet()) {
                QueryStatusResp status = entry.getValue();
                log.info("  Node {}: leaderId={}, term={}, groupReady={}, members={}, observers={}",
                        entry.getKey(), status.leaderId, status.term, status.isGroupReady(),
                        status.members, status.observers);
            }
        }
    }

    /**
     * Validate full cluster consistency including leader, term, members, observers, and groupReady
     */
    private boolean validateFullConsistency(Map<Integer, QueryStatusResp> allStatus) {
        if (allStatus.isEmpty()) {
            log.warn("No status available for full consistency check");
            return false;
        }

        HashSet<Integer> leaderIds = new HashSet<>();
        HashSet<Integer> terms = new HashSet<>();
        HashSet<HashSet<Integer>> membersSets = new HashSet<>();
        HashSet<HashSet<Integer>> observersSets = new HashSet<>();
        boolean allGroupReady = true;

        for (Map.Entry<Integer, QueryStatusResp> entry : allStatus.entrySet()) {
            QueryStatusResp status = entry.getValue();

            leaderIds.add(status.leaderId);
            terms.add(status.term);
            membersSets.add(new HashSet<>(status.members));
            observersSets.add(new HashSet<>(status.observers));

            if (!status.isGroupReady()) {
                allGroupReady = false;
                log.debug("Node {} is not groupReady yet", entry.getKey());
            }

            Assertions.assertFalse(status.isBug(), "Node " + entry.getKey() + " has bug flag set");
        }

        boolean leaderConsistent = leaderIds.size() == 1;
        boolean termConsistent = terms.size() == 1;
        boolean membersConsistent = membersSets.size() == 1;
        boolean observersConsistent = observersSets.size() == 1;

        if (leaderConsistent && termConsistent && membersConsistent && observersConsistent && allGroupReady) {
            int leaderId = leaderIds.iterator().next();
            return leaderId > 0 && allStatus.get(leaderId) != null;
        }

        return false;
    }

    /**
     * Close the AdminRaftClient
     */
    public void close() {
        try {
            if (adminClient.getStatus() == AdminRaftClient.STATUS_RUNNING) {
                adminClient.stop(new DtTime(5, TimeUnit.SECONDS));
            }
            log.info("AdminRaftClient stopped");
        } catch (Exception e) {
            log.warn("Error stopping AdminRaftClient", e);
        }
    }
}
