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

import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class RaftUtil {
    private static final DtLog log = DtLogs.getLogger(RaftUtil.class);
    public final static ScheduledExecutorService SCHEDULED_SERVICE = Executors.newSingleThreadScheduledExecutor();

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
                int x = hostPortStr.lastIndexOf(':');
                if (x < 0 || x == hostPortStr.length() - 1) {
                    throw new IllegalArgumentException("not 'id,host:port' format:" + server);
                }
                String host = hostPortStr.substring(0, x).trim();
                if (host.startsWith("[") && host.endsWith("]")) {
                    host = host.substring(1, host.length() - 1);
                }
                int port = Integer.parseInt(hostPortStr.substring(x + 1).trim());
                boolean self = id == selfId;
                list.add(new RaftNode(id, new HostPort(host, port), self));
            }
            return list;
        } catch (NumberFormatException e) {
            throw new RaftException("bad servers list: " + serversStr);
        }
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    public static boolean needCommit(long currentCommitIndex, long recentMatchIndex,
                                     List<RaftMember> servers, int rwQuorum) {
        if (recentMatchIndex < currentCommitIndex) {
            return false;
        }
        int count = 0;
        for (int i = 0; i < servers.size(); i++) {
            RaftMember member = servers.get(i);
            if (member.getNode().isSelf()) {
                if (recentMatchIndex > member.getMatchIndex()) {
                    return false;
                }
            }
            if (member.getMatchIndex() >= recentMatchIndex) {
                count++;
            }
        }
        return count >= rwQuorum;
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    public static void updateLease(long currentReqNanos, RaftStatus raftStatus) {
        int order = 0;
        List<RaftMember> allMembers = raftStatus.getMembers();
        int len = allMembers.size();
        for (int i = 0; i < len; i++) {
            RaftMember node = allMembers.get(i);
            if (!node.isHasLastConfirmReqNanos()) {
                continue;
            }
            if (currentReqNanos - node.getLastConfirmReqNanos() <= 0) {
                order++;
            }
        }
        if (raftStatus.getRwQuorum() == order) {
            raftStatus.setLeaseStartNanos(currentReqNanos);
        }
    }

    public static void resetElectTimer(RaftStatus raftStatus) {
        raftStatus.setLastElectTime(raftStatus.getTs().getNanoTime());
    }

    public static void resetStatus(RaftStatus raftStatus) {
        raftStatus.setFirstCommitIndexOfCurrentTerm(0);
        raftStatus.setFirstCommitOfApplied(new CompletableFuture<>());
        RaftUtil.resetElectTimer(raftStatus);
        raftStatus.setHeartbeatTime(raftStatus.getLastElectTime());
        raftStatus.setLeaseStartNanos(0);
        raftStatus.setPendingRequests(new PendingMap());
        raftStatus.setCurrentLeader(null);
        for (RaftMember node : raftStatus.getMembers()) {
            node.setMatchIndex(0);
            node.setNextIndex(0);
            node.setPendingStat(new PendingStat());
            node.setLastConfirm(false, 0);
            node.setMultiAppend(false);
            node.setInstallSnapshot(false);
            if (node.getSnapshotInfo() != null) {
                try {
                    SnapshotInfo si = node.getSnapshotInfo();
                    if (si.iterator != null) {
                        si.stateMachine.closeIterator(si.iterator);
                    }
                } catch (Exception e) {
                    log.error("close snapshot error", e);
                }
            }
            node.setSnapshotInfo(null);
        }
    }

    public static void incrTermAndConvertToFollower(int remoteTerm, RaftStatus raftStatus,
                                                    int newLeaderId, boolean persist) {
        RaftRole oldRole = raftStatus.getRole();
        if (oldRole != RaftRole.observer) {
            log.info("update term from {} to {}, change to follower, oldRole={}",
                    raftStatus.getCurrentTerm(), remoteTerm, raftStatus.getRole());
            PendingMap oldPending = raftStatus.getPendingRequests();
            resetStatus(raftStatus);
            if (newLeaderId > 0) {
                updateLeader(raftStatus, newLeaderId);
            }
            raftStatus.setCurrentTerm(remoteTerm);
            raftStatus.setVotedFor(0);
            raftStatus.setRole(RaftRole.follower);
            if (oldRole == RaftRole.leader) {
                oldPending.forEach((idx, task) -> {
                    RaftNode leaderNode = getLeader(raftStatus.getCurrentLeader());
                    if (task.future != null) {
                        task.future.completeExceptionally(new NotLeaderException(leaderNode));
                    }
                    if (task.nextReaders != null) {
                        task.nextReaders.forEach(readTask -> {
                            if (readTask.future != null) {
                                readTask.future.completeExceptionally(new NotLeaderException(leaderNode));
                            }
                        });
                    }
                    return true;
                });
            }
        } else {
            log.info("update term from {} to {}", raftStatus.getCurrentTerm(), remoteTerm);
            raftStatus.setCurrentTerm(remoteTerm);
            if (newLeaderId > 0) {
                updateLeader(raftStatus, newLeaderId);
            }
        }
        if (persist) {
            StatusUtil.updateStatusFile(raftStatus);
        }
    }

    public static void changeToFollower(RaftStatus raftStatus, int leaderId) {
        log.info("change to follower. term={}, oldRole={}", raftStatus.getCurrentTerm(), raftStatus.getRole());
        resetStatus(raftStatus);
        if (leaderId > 0) {
            updateLeader(raftStatus, leaderId);
        }
        raftStatus.setRole(RaftRole.follower);
    }

    public static void changeToLeader(RaftStatus raftStatus) {
        log.info("change to leader. term={}", raftStatus.getCurrentTerm());
        resetStatus(raftStatus);
        raftStatus.setRole(RaftRole.leader);
        for (RaftMember node : raftStatus.getMembers()) {
            node.setNextIndex(raftStatus.getLastLogIndex() + 1);
        }
    }

    public static void updateLeader(RaftStatus raftStatus, int leaderId) {
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
            raftStatus.setCurrentLeader(null);
        }
    }

    public static RaftNode getLeader(RaftMember leader) {
        return leader == null ? null : leader.getNode();
    }

    public static void append(RaftLog raftLog, RaftStatus raftStatus, long prevLogIndex,
                              int prevLogTerm, ArrayList<LogItem> logs) {
        RaftUtil.doWithSyncRetry(() -> {
            raftLog.append(prevLogIndex, prevLogTerm, logs);
            return null;
        }, raftStatus, 1000, "raft log append error");
    }

    public static <T> T doWithSyncRetry(Supplier<T> callback, RaftStatus raftStatus, long sleepMillis, String errorMsg) {
        int failCount = 0;
        while (true) {
            try {
                T result = callback.get();
                if (failCount > 0) {
                    raftStatus.setError(false);
                }
                return result;
            } catch (Exception e) {
                failCount++;
                log.error(errorMsg, e);
                if (failCount == 2) {
                    raftStatus.setError(true);
                }
                try {
                    //noinspection BusyWait
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException ex) {
                    throw new RaftException(ex);
                }
            }
        }
    }

    public static LogItem[] load(RaftLog raftLog, RaftStatus raftStatus, long index, int limit, long bytesLimit) {
        LogItem[] items;
        items = doWithSyncRetry(() -> raftLog.load(index, limit, bytesLimit),
                raftStatus, 1000, "raft log load error");
        if (items == null || items.length == 0) {
            throw new RaftException("can't load raft log, result is null or empty. index=" + index +
                    ", limit=" + limit + ", bytesLimit=" + bytesLimit);
        }
        return items;
    }

    public static GroupComponents getGroupComponents(IntObjMap<GroupComponents> groupComponentsMap, int groupId) {
        GroupComponents gc = groupComponentsMap.get(groupId);
        if (gc == null) {
            log.error("raft group not found: {}", groupId);
            throw new NetCodeException(CmdCodes.SYS_ERROR, "raft group not found: " + groupId);
        }
        return gc;
    }

    public static int getElectQuorum(int groupSize) {
        return groupSize / 2 + 1;
    }

    public static int getRwQuorum(int groupSize) {
        return groupSize >= 4 && groupSize % 2 == 0 ? groupSize / 2 : groupSize / 2 + 1;
    }
}
