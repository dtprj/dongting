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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.FlowControlException;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespFrame;
import com.github.dtprj.dongting.net.NioNet;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftNode;
import com.github.dtprj.dongting.raft.store.RaftLog;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
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

    public static void updateLease(RaftStatusImpl raftStatus) {
        long leaseStartTime = computeLease(raftStatus, raftStatus.getElectQuorum(), raftStatus.getMembers());
        List<RaftMember> jointMembers = raftStatus.getPreparedMembers();
        if (jointMembers.size() > 0) {
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

    public static void resetElectTimer(RaftStatusImpl raftStatus) {
        raftStatus.setLastElectTime(raftStatus.getTs().getNanoTime());
    }

    public static void closeIterator(RaftMember member) {
        RaftLog.LogIterator it = member.getReplicateIterator();
        if (it != null) {
            DtUtil.close(it);
            member.setReplicateIterator(null);
        }
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
        for (RaftMember member : raftStatus.getReplicateList()) {
            member.setMatchIndex(0);
            member.setNextIndex(0);
            member.setPendingStat(new PendingStat());
            member.setLastConfirmReqNanos(raftStatus.getTs().getNanoTime() - Duration.ofDays(1).toNanos());
            member.setMultiAppend(false);
            member.setInstallSnapshot(false);
            member.incrReplicateEpoch(member.getReplicateEpoch());
            if (member.getReplicateFuture() != null) {
                member.getReplicateFuture().cancel(false);
            }
            closeIterator(member);
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

    public static void changeToFollower(RaftStatusImpl raftStatus, int leaderId) {
        log.info("change to follower. term={}, oldRole={}", raftStatus.getCurrentTerm(), raftStatus.getRole());
        resetStatus(raftStatus);
        if (leaderId > 0) {
            updateLeader(raftStatus, leaderId);
        }
        raftStatus.setRole(RaftRole.follower);
    }

    public static void changeToObserver(RaftStatusImpl raftStatus, int leaderId) {
        log.info("change to observer. term={}, oldRole={}", raftStatus.getCurrentTerm(), raftStatus.getRole());
        resetStatus(raftStatus);
        if (leaderId > 0) {
            updateLeader(raftStatus, leaderId);
        }
        raftStatus.setRole(RaftRole.observer);
    }

    public static void changeToLeader(RaftStatusImpl raftStatus) {
        log.info("change to leader. term={}", raftStatus.getCurrentTerm());
        resetStatus(raftStatus);
        raftStatus.setRole(RaftRole.leader);
        raftStatus.setFirstIndexOfCurrentTerm(raftStatus.getLastLogIndex() + 1);
        for (RaftMember node : raftStatus.getReplicateList()) {
            node.setNextIndex(raftStatus.getLastLogIndex() + 1);
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

    public static int getElectQuorum(int groupSize) {
        return groupSize / 2 + 1;
    }

    public static int getRwQuorum(int groupSize) {
        return groupSize >= 4 && groupSize % 2 == 0 ? groupSize / 2 : groupSize / 2 + 1;
    }

    public static <T> Set<T> union(Collection<T> c1, Collection<T> c2) {
        HashSet<T> set = new HashSet<>();
        set.addAll(c1);
        set.addAll(c2);
        return set;
    }

    public static ByteBuffer copy(ByteBuffer src) {
        ByteBuffer dest = ByteBuffer.allocate(src.remaining());
        int pos = src.position();
        dest.put(src);
        dest.flip();
        src.position(pos);
        return dest;
    }

    public static void checkStop(Supplier<Boolean> stopIndicator) {
        if (stopIndicator.get()) {
            throw new StoppedException();
        }
    }

    public static void updateCrc(CRC32C crc32c, ByteBuffer buf, int startPos, int len) {
        int oldPos = buf.position();
        int oldLimit = buf.limit();
        buf.limit(startPos + len);
        buf.position(startPos);
        crc32c.update(buf);
        buf.limit(oldLimit);
        buf.position(oldPos);
    }

    public static void processError(ReadFrame<?> frame, ChannelContext channelContext,
                                    ReqContext reqContext, Throwable ex) {
        if (ex instanceof RaftExecTimeoutException) {
            log.warn("raft operation timeout");
            return;
        }
        if (ex instanceof CompletionException) {
            ex = ex.getCause();
        }
        EmptyBodyRespFrame errorResp;
        if (ex instanceof FlowControlException) {
            errorResp = new EmptyBodyRespFrame(CmdCodes.FLOW_CONTROL);
        } else if (ex instanceof NotLeaderException) {
            errorResp = new EmptyBodyRespFrame(CmdCodes.NOT_RAFT_LEADER);
            RaftNode leader = ((NotLeaderException) ex).getCurrentLeader();
            if (leader != null) {
                String hpStr = leader.getHostPort().getHost() + ":" + leader.getHostPort().getPort();
                errorResp.setExtra(hpStr.getBytes(StandardCharsets.UTF_8));
            }
            log.warn("not leader, current leader is {}", leader);
        } else {
            errorResp = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
            log.warn("raft processor error: {}", ex.toString());
        }
        errorResp.setMsg(ex.toString());
        channelContext.getRespWriter().writeRespInBizThreads(frame, errorResp, reqContext.getTimeout());
    }
}
