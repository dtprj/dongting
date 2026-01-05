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
public class Validator {
    private static final Logger log = LoggerFactory.getLogger(Validator.class);

    private static final long LEADER_ELECTION_TIMEOUT_SECONDS = 60;
    private static final long QUERY_RETRY_INTERVAL_MS = 100;
    private static final long SINGLE_QUERY_TIMEOUT_SECONDS = 2;

    private AdminRaftClient adminClient;
    private final int groupId;
    private final int[] nodeIds;

    public Validator(int groupId, int[] nodeIds) {
        this.groupId = groupId;
        this.nodeIds = nodeIds;
    }

    /**
     * Initialize AdminRaftClient and configure cluster
     */
    public void initialize() {
        String servers = ItUtil.formatReplicateServers(nodeIds);
        log.info("Initializing AdminRaftClient with servers: {}", servers);

        adminClient = new AdminRaftClient();
        adminClient.start();

        // Add node information
        adminClient.clientAddNode(servers);

        // Add group information
        adminClient.clientAddOrUpdateGroup(groupId, nodeIds);

        log.info("AdminRaftClient initialized successfully");
    }

    /**
     * Wait for leader election to complete
     */
    public void waitForLeaderElection() throws Exception {
        waitForLeaderElection(LEADER_ELECTION_TIMEOUT_SECONDS);
    }

    /**
     * Wait for leader election with timeout
     */
    public void waitForLeaderElection(long timeoutSeconds) throws Exception {
        log.info("Waiting for leader election (timeout: {} seconds)", timeoutSeconds);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        int attemptCount = 0;

        while (System.nanoTime() - deadline < 0) {
            attemptCount++;

            try {
                // Query all nodes
                Map<Integer, QueryStatusResp> allStatus = queryAllNodeStatus();

                // Validate leader consistency
                if (validateLeaderConsistency(allStatus)) {
                    log.info("Leader election completed successfully");
                    return;
                } else {
                    log.warn("Leader consistency validation failed, will retry");
                }

            } catch (Exception e) {
                log.debug("Failed to query status (attempt {}): {}", attemptCount, e.getMessage());
            }

            // Wait before retry
            Thread.sleep(QUERY_RETRY_INTERVAL_MS);
        }

        // Timeout - collect final status for diagnosis
        Map<Integer, QueryStatusResp> finalStatus = queryAllNodeStatus();
        log.error("Leader election timeout after {} attempts. Final status:", attemptCount);
        for (Map.Entry<Integer, QueryStatusResp> entry : finalStatus.entrySet()) {
            QueryStatusResp status = entry.getValue();
            log.error("  Node {}: term={}, leaderId={}, members={}",
                    entry.getKey(), status.term, status.leaderId, status.members);
        }

        throw new RuntimeException("Leader election timeout after " + timeoutSeconds + " seconds");
    }

    /**
     * Query status from all nodes
     */
    public Map<Integer, QueryStatusResp> queryAllNodeStatus() {
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
     * Validate that all nodes agree on the leader
     */
    public boolean validateLeaderConsistency(Map<Integer, QueryStatusResp> allStatus) {
        if (allStatus.isEmpty()) {
            log.warn("No status available for consistency check");
            return false;
        }
        HashSet<Integer> leaderIds = new HashSet<>();
        HashSet<Integer> terms = new HashSet<>();

        for (Map.Entry<Integer, QueryStatusResp> entry : allStatus.entrySet()) {
            QueryStatusResp status = entry.getValue();
            // Check leader consistency
            if (status.leaderId > 0) {
                leaderIds.add(status.leaderId);
            }
            terms.add(status.term);
            Assertions.assertFalse(status.isBug(), "Node " + entry.getKey() + " has bug flag set");
        }
        if (leaderIds.size() == 1 && terms.size() == 1) {
            int leaderId = leaderIds.stream().findFirst().get();
            int term = terms.stream().findFirst().get();
            log.info("Leader consistency validated: leaderId={}, term={}", leaderId, term);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Close the AdminRaftClient
     */
    public void close() {
        if (adminClient != null) {
            try {
                adminClient.stop(new DtTime(5, TimeUnit.SECONDS));
                log.info("AdminRaftClient stopped");
            } catch (Exception e) {
                log.warn("Error stopping AdminRaftClient", e);
            }
        }
    }
}
