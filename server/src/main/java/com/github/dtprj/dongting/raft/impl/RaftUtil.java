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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioNet;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftNode;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class RaftUtil {
    private static final DtLog log = DtLogs.getLogger(RaftUtil.class);
    public final static ScheduledExecutorService SCHEDULED_SERVICE = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "DtRaftSchedule");
        t.setDaemon(true);
        return t;
    });

    public static void updateCrc(CRC32C crc32c, ByteBuffer buf, int startPos, int len) {
        if (len == 0) {
            return;
        }
        int oldPos = buf.position();
        int oldLimit = buf.limit();
        buf.limit(startPos + len);
        buf.position(startPos);
        crc32c.update(buf);
        buf.limit(oldLimit);
        buf.position(oldPos);
    }

    public static void checkStop(FiberGroup fiberGroup) {
        if (fiberGroup.isShouldStop()) {
            throw new RaftException("raft group stopped");
        }
    }

    public static List<RaftNode> parseServers(int selfId, String serversStr) {
        String[] servers = serversStr.split(";");
        if (servers.length == 0) {
            throw new RaftException("servers list is empty");
        }
        try {
            List<RaftNode> list = new ArrayList<>();
            for (String server : servers) {
                String[] arr = server.split(",");
                if (arr.length != 2) {
                    throw new IllegalArgumentException("not 'id,host:port' format:" + server);
                }
                int id = Integer.parseInt(arr[0].trim());
                String hostPortStr = arr[1];
                list.add(new RaftNode(id, NioNet.parseHostPort(hostPortStr), id == selfId));
            }
            return list;
        } catch (NumberFormatException e) {
            throw new RaftException("bad servers list: " + serversStr);
        }
    }

    public static int getElectQuorum(int groupSize) {
        return groupSize / 2 + 1;
    }

    public static int getRwQuorum(int groupSize) {
        return groupSize >= 4 && groupSize % 2 == 0 ? groupSize / 2 : groupSize / 2 + 1;
    }

    public static void release(List<LogItem> items) {
        if (items == null) {
            return;
        }
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < items.size(); i++) {
            items.get(i).release();
        }
    }

    public static void incrTerm(int remoteTerm, RaftStatusImpl raftStatus, int newLeaderId) {
        RaftRole oldRole = raftStatus.getRole();
        if (oldRole != RaftRole.observer) {
            log.info("update term from {} to {}, change to follower, oldRole={}",
                    raftStatus.getCurrentTerm(), remoteTerm, raftStatus.getRole());
            TailCache oldPending = raftStatus.getTailCache();
            resetStatus(raftStatus);
            raftStatus.setRole(RaftRole.follower);
            if (oldRole == RaftRole.leader) {
                oldPending.forEach((idx, task) -> {
                    RaftNode leaderNode = raftStatus.getCurrentLeaderNode();
                    if (task.getFuture() != null) {
                        task.getFuture().completeExceptionally(new NotLeaderException(leaderNode));
                    }
                    RaftTask reader;
                    while ((reader = task.getNextReader()) != null) {
                        if (reader.getFuture() != null) {
                            reader.getFuture().completeExceptionally(new NotLeaderException(leaderNode));
                        }
                    }
                });
            }
        } else {
            log.info("update term from {} to {}", raftStatus.getCurrentTerm(), remoteTerm);
            resetStatus(raftStatus);
        }
        if (newLeaderId > 0) {
            updateLeader(raftStatus, newLeaderId);
        }
        raftStatus.setCurrentTerm(remoteTerm);
        raftStatus.setVotedFor(0);
    }

    public static void resetStatus(RaftStatusImpl raftStatus) {
        raftStatus.setFirstIndexOfCurrentTerm(0);
        raftStatus.setFirstCommitOfApplied(new CompletableFuture<>());
        RaftUtil.resetElectTimer(raftStatus);
        raftStatus.setHeartbeatTime(raftStatus.getLastElectTime());
        raftStatus.setLeaseStartNanos(0);
        raftStatus.setTailCache(new TailCache());
        raftStatus.setCurrentLeader(null);
        raftStatus.setHoldRequest(false);
        raftStatus.setLeaderCommit(0);

        // wake up replicate fiber if it is waiting on this condition
        raftStatus.getDataArrivedCondition().signalAll();

        for (RaftMember member : raftStatus.getReplicateList()) {
            member.setMatchIndex(0);
            member.setNextIndex(0);
            member.setLastConfirmReqNanos(raftStatus.getTs().getNanoTime() - Duration.ofDays(1).toNanos());
            member.setInstallSnapshot(false);
            member.incrReplicateEpoch(member.getReplicateEpoch());
            // wake up replicate fiber if it is waiting on this condition
            raftStatus.getReplicateCondition(member.getNode().getNodeId()).signalAll();
            if (member.getSnapshotInfo() != null) {
                try {
                    SnapshotInfo si = member.getSnapshotInfo();
                    if (si != null && si.snapshot != null) {
                        si.snapshot.close();
                    }
                } catch (Exception e) {
                    log.error("close snapshot error", e);
                }
            }
            member.setSnapshotInfo(null);
        }
    }

    public static void updateLeader(RaftStatusImpl raftStatus, int leaderId) {
        RaftMember leader = raftStatus.getCurrentLeader();
        if (leader != null && leader.getNode().getNodeId() == leaderId) {
            return;
        }
        boolean found = false;
        for (RaftMember node : raftStatus.getMembers()) {
            if (node.getNode().getNodeId() == leaderId) {
                raftStatus.setCurrentLeader(node);
                found = true;
            }
        }
        if (!found) {
            for (RaftMember node : raftStatus.getPreparedMembers()) {
                if (node.getNode().getNodeId() == leaderId) {
                    raftStatus.setCurrentLeader(node);
                    found = true;
                }
            }
        }
        if (!found) {
            raftStatus.setCurrentLeader(null);
        }
    }

    public static void resetElectTimer(RaftStatusImpl raftStatus) {
        raftStatus.setLastElectTime(raftStatus.getTs().getNanoTime());
    }

    public static void updateLease(RaftStatusImpl raftStatus) {
        long leaseStartTime = computeLease(raftStatus, raftStatus.getElectQuorum(), raftStatus.getMembers());
        List<RaftMember> jointMembers = raftStatus.getPreparedMembers();
        if (!jointMembers.isEmpty()) {
            long lease2 = computeLease(raftStatus, RaftUtil.getElectQuorum(jointMembers.size()), jointMembers);
            if (lease2 - leaseStartTime < 0) {
                leaseStartTime = lease2;
            }
        }
        if (leaseStartTime != raftStatus.getLeaseStartNanos()) {
            raftStatus.setLeaseStartNanos(leaseStartTime);
        }
    }

    /**
     * test if currentReqNanos is the X(X=quorum)th large value in list(lastConfirmReqNanos).
     */
    private static long computeLease(RaftStatusImpl raftStatus, int quorum, List<RaftMember> list) {
        int len = list.size();
        if (len == 1) {
            return list.get(0).getLastConfirmReqNanos();
        }
        long[] arr = raftStatus.getLeaseComputeArray();
        if (arr.length < len) {
            arr = new long[len];
            raftStatus.setLeaseComputeArray(arr);
        }
        for (int i = 0; i < len; i++) {
            RaftMember m = list.get(i);
            arr[i] = m.getLastConfirmReqNanos();
        }
        for (int i = 0; i < quorum; i++) {
            if (arr[i] - arr[i + 1] < 0) {
                long tmp = arr[i];
                arr[i] = arr[i + 1];
                arr[i + 1] = tmp;
            }
        }
        return arr[quorum];
    }
}
