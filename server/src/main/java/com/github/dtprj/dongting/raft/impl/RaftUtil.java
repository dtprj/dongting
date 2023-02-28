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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftLog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author huangli
 */
public class RaftUtil {
    private static final DtLog log = DtLogs.getLogger(RaftUtil.class);

    public static Set<HostPort> parseServers(String serversStr) {
        Set<HostPort> servers = Arrays.stream(serversStr.split("[,;]"))
                .filter(Objects::nonNull)
                .map(s -> {
                    String[] arr = s.split(":");
                    if (arr.length != 2) {
                        throw new IllegalArgumentException("not 'host:port' format:" + s);
                    }
                    return new HostPort(arr[0].trim(), Integer.parseInt(arr[1].trim()));
                }).collect(Collectors.toSet());
        if (servers.size() == 0) {
            throw new RaftException("servers list is empty");
        }
        return servers;
    }

    public static boolean needCommit(long currentCommitIndex, long recentMatchIndex,
                                     List<RaftNode> servers, int rwQuorum) {
        if (recentMatchIndex < currentCommitIndex) {
            return false;
        }
        int count = 0;
        for (RaftNode node : servers) {
            if (node.isSelf()) {
                if (recentMatchIndex > node.getMatchIndex()) {
                    return false;
                }
            }
            if (node.getMatchIndex() >= recentMatchIndex) {
                count++;
            }
        }
        return count >= rwQuorum;
    }

    public static void updateLease(long currentReqNanos, RaftStatus raftStatus) {
        int order = 0;
        for (RaftNode node : raftStatus.getServers()) {
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
        for (RaftNode node : raftStatus.getServers()) {
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

    public static void incrTermAndConvertToFollower(int remoteTerm, RaftStatus raftStatus, int newLeaderId) {
        log.info("update term from {} to {}, change to follower, oldRole={}",
                raftStatus.getCurrentTerm(), remoteTerm, raftStatus.getRole());
        RaftRole oldRole = raftStatus.getRole();
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
                HostPort leaderHostPort = getLeader(raftStatus.getCurrentLeader());
                if (task.future != null) {
                    task.future.completeExceptionally(new NotLeaderException(leaderHostPort));
                }
                if (task.nextReaders != null) {
                    task.nextReaders.forEach(readTask -> {
                        if (readTask.future != null) {
                            readTask.future.completeExceptionally(new NotLeaderException(leaderHostPort));
                        }
                    });
                }
                return true;
            });
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
        for (RaftNode node : raftStatus.getServers()) {
            node.setNextIndex(raftStatus.getLastLogIndex() + 1);
        }
    }

    public static void updateLeader(RaftStatus raftStatus, int leaderId) {
        RaftNode leader = raftStatus.getCurrentLeader();
        if (leader != null && leader.getId() == leaderId) {
            return;
        }
        for (RaftNode node : raftStatus.getServers()) {
            if (node.getId() == leaderId) {
                raftStatus.setCurrentLeader(node);
            }
        }
    }

    public static HostPort getLeader(RaftNode leader) {
        return leader == null ? null : leader.getPeer().getEndPoint();
    }

    public static void append(RaftLog raftLog, RaftStatus raftStatus, long prevLogIndex,
                              int prevLogTerm, ArrayList<LogItem> logs) {
        RaftUtil.doWithRetry(() -> {
            raftLog.append(prevLogIndex, prevLogTerm, logs);
            return null;
        }, raftStatus, 1000, "raft log append error");
    }

    public static <T> T doWithRetry(Supplier<T> callback, RaftStatus raftStatus, long sleepMillis, String errorMsg) {
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
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException ex) {
                    throw new RaftException(ex);
                }
            }
        }
    }

    public static LogItem[] load(RaftLog raftLog, RaftStatus raftStatus, long index, int limit, long bytesLimit) {
        LogItem[] items;
        items = doWithRetry(() -> raftLog.load(index, limit, bytesLimit),
                raftStatus, 1000, "raft log load error");
        if (items == null || items.length == 0) {
            throw new RaftException("can't load raft log, result is null or empty. index=" + index +
                    ", limit=" + limit + ", bytesLimit=" + bytesLimit);
        }
        return items;
    }
}
