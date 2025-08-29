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

import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.RefCount;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCall;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.rpc.RaftPing;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftInput;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public final class RaftUtil {
    private static final DtLog log = DtLogs.getLogger(RaftUtil.class);

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

    public static int getElectQuorum(int groupSize) {
        return (groupSize >> 1) + 1;
    }

    public static int getRwQuorum(int groupSize) {
        if (groupSize >= 4 && groupSize % 2 == 0) {
            return groupSize >> 1;
        } else {
            return (groupSize >> 1) + 1;
        }
    }

    public static void release(RaftInput raftInput) {
        if (raftInput.isHeadReleasable()) {
            ((RefCount) raftInput.getHeader()).release();
        }
        if (raftInput.isBodyReleasable()) {
            ((RefCount) raftInput.getBody()).release();
        }
    }

    public static void release(List<LogItem> items) {
        if (items == null) {
            return;
        }
        for (int s = items.size(), i = 0; i < s; i++) {
            items.get(i).release();
        }
    }

    public static void incrTerm(int remoteTerm, RaftStatusImpl raftStatus, int newLeaderId, String reason) {
        RaftRole oldRole = raftStatus.getRole();
        if (newLeaderId > 0) {
            updateLeader(raftStatus, newLeaderId);
        }
        LinkedList<Pair<RaftTask, NotLeaderException>> failList = new LinkedList<>();
        if (oldRole != RaftRole.observer && oldRole != RaftRole.none) {
            log.info("update term from {} to {}, change to follower, oldRole={}, reason: {}",
                    raftStatus.currentTerm, remoteTerm, raftStatus.getRole(), reason);
            raftStatus.setRole(RaftRole.follower);
            if (oldRole == RaftRole.leader) {
                TailCache oldPending = raftStatus.tailCache;
                NotLeaderException e = new NotLeaderException(raftStatus.getCurrentLeaderNode());
                oldPending.forEach((idx, task) -> failList.add(new Pair<>(task, e)));
            }
        } else {
            log.info("update term from {} to {}, reason: {}", raftStatus.currentTerm, remoteTerm, reason);
        }
        resetStatus(raftStatus);
        raftStatus.currentTerm = remoteTerm;
        raftStatus.votedFor = 0;
        raftStatus.copyShareStatus();
        // copy share status should happen before callback invocation
        for (Pair<RaftTask, NotLeaderException> pair : failList) {
            pair.getLeft().callFail(pair.getRight());
        }
    }

    public static void resetStatus(RaftStatusImpl raftStatus) {
        resetStatus(raftStatus, true);
    }

    private static void resetStatus(RaftStatusImpl raftStatus, boolean cleanLastConfirmReqNanos) {
        raftStatus.groupReadyIndex = Long.MAX_VALUE;
        raftStatus.setGroupReady(false);
        RaftUtil.resetElectTimer(raftStatus);
        raftStatus.setLeaseStartNanos(0);
        raftStatus.setCurrentLeader(null);
        raftStatus.leaderCommit = 0;

        raftStatus.commitHistory = new IndexedQueue<>(16);
        raftStatus.setApplyLagNanos(TimeUnit.DAYS.toNanos(1));

        // wake up replicate fiber if it is waiting on this condition
        raftStatus.needRepCondition.signalAll();

        clearTransferLeaderCondition(raftStatus);

        for (RaftMember member : raftStatus.replicateList) {
            member.matchIndex = 0;
            member.nextIndex = 0;
            member.repCommitIndex = 0;
            member.repCommitIndexAcked = 0;
            if (cleanLastConfirmReqNanos) {
                member.lastConfirmReqNanos = raftStatus.ts.getNanoTime() - Duration.ofDays(1).toNanos();
            }

            member.installSnapshot = false;
            member.replicateEpoch++;
            // wake up replicate fiber if it is waiting on this condition
            member.repDoneCondition.signalAll();
        }
    }

    public static void clearTransferLeaderCondition(RaftStatusImpl raftStatus) {
        if (raftStatus.transferLeaderCondition != null) {
            raftStatus.transferLeaderCondition.signalAll();
            raftStatus.transferLeaderCondition = null;
        }
    }

    public static void updateLeader(RaftStatusImpl raftStatus, int leaderId) {
        RaftMember leader = raftStatus.getCurrentLeader();
        if (leader != null && leader.node.nodeId == leaderId) {
            return;
        }
        boolean found = false;
        for (RaftMember node : raftStatus.members) {
            if (node.node.nodeId == leaderId) {
                raftStatus.setCurrentLeader(node);
                found = true;
            }
        }
        if (!found) {
            for (RaftMember node : raftStatus.preparedMembers) {
                if (node.node.nodeId == leaderId) {
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
        raftStatus.ts.refresh(1);
        raftStatus.lastElectTime = raftStatus.ts.nanoTime;
    }

    public static void updateLease(RaftStatusImpl raftStatus) {
        long leaseStartTime = computeLease(raftStatus, raftStatus.rwQuorum, raftStatus.members);
        List<RaftMember> jointMembers = raftStatus.preparedMembers;
        if (!jointMembers.isEmpty()) {
            long lease2 = computeLease(raftStatus, RaftUtil.getRwQuorum(jointMembers.size()), jointMembers);
            if (lease2 - leaseStartTime < 0) {
                leaseStartTime = lease2;
            }
        }
        raftStatus.setLeaseStartNanos(leaseStartTime);
    }

    /**
     * return the X(X=quorum)th large(lastConfirmReqNanos) value in list.
     */
    private static long computeLease(RaftStatusImpl raftStatus, int quorum, List<RaftMember> list) {
        int len = list.size();
        if (len == 1) {
            return list.get(0).lastConfirmReqNanos;
        }
        long[] arr = raftStatus.leaseComputeArray;
        if (arr.length != len) {
            arr = new long[len];
            raftStatus.leaseComputeArray = arr;
        }
        for (int i = 0; i < len; i++) {
            RaftMember m = list.get(i);
            arr[i] = m.lastConfirmReqNanos;
        }
        for (int i = 0; i < quorum; i++) {
            // sort desc
            for (int j = len - 1; j > 0; j--) {
                if (arr[j - 1] - arr[j] < 0) {
                    long tmp = arr[j];
                    arr[j] = arr[j - 1];
                    arr[j - 1] = tmp;
                }
            }
        }
        return arr[quorum - 1];
    }

    public static ByteBuffer copy(ByteBuffer src) {
        ByteBuffer dest = ByteBuffer.allocate(src.remaining());
        int pos = src.position();
        dest.put(src);
        dest.flip();
        src.position(pos);
        return dest;
    }

    public static void changeToFollower(RaftStatusImpl raftStatus, int leaderId, String reason) {
        log.info("change to follower. term={}, oldRole={}, reason: {}", raftStatus.currentTerm,
                raftStatus.getRole(), reason);
        resetStatus(raftStatus);
        updateLeader(raftStatus, leaderId);
        raftStatus.setRole(RaftRole.follower);
        raftStatus.copyShareStatus();
    }

    public static void changeToObserver(RaftStatusImpl raftStatus, int leaderId) {
        log.info("change to observer. term={}, oldRole={}", raftStatus.currentTerm, raftStatus.getRole());
        resetStatus(raftStatus);
        updateLeader(raftStatus, leaderId);
        raftStatus.setRole(RaftRole.observer);
        raftStatus.copyShareStatus();
    }

    public static void changeToNone(RaftStatusImpl raftStatus, int leaderId) {
        log.info("change to none. term={}, oldRole={}", raftStatus.currentTerm, raftStatus.getRole());
        resetStatus(raftStatus);
        updateLeader(raftStatus, leaderId);
        raftStatus.setRole(RaftRole.none);
        raftStatus.copyShareStatus();
    }

    public static void changeToLeader(RaftStatusImpl raftStatus) {
        resetStatus(raftStatus, false);
        raftStatus.setRole(RaftRole.leader);
        raftStatus.setCurrentLeader(raftStatus.self);
        raftStatus.groupReadyIndex = raftStatus.lastLogIndex + 1;
        for (RaftMember node : raftStatus.replicateList) {
            node.nextIndex = raftStatus.lastLogIndex + 1;
        }
        updateLease(raftStatus);
        raftStatus.copyShareStatus();
        long restLeaseMillis = (raftStatus.getShareStatus().leaseEndNanos - raftStatus.ts.nanoTime) / 1_000_000;
        log.info("change to leader. groupId={}, term={}, lastLogIndex={}, lease rest {}ms",
                raftStatus.groupId, raftStatus.currentTerm, raftStatus.lastLogIndex, restLeaseMillis);
    }

    public static boolean writeNotFinished(RaftStatusImpl raftStatus) {
        return raftStatus.lastForceLogIndex != raftStatus.lastLogIndex || raftStatus.truncating;
    }

    public static FrameCallResult waitWriteFinish(RaftStatusImpl raftStatus, FrameCall<Void> resumePoint) {
        if (writeNotFinished(raftStatus)) {
            log.info("write not finished, lastPersistLogIndex={}, lastLogIndex={}, truncating={}",
                    raftStatus.lastForceLogIndex, raftStatus.lastLogIndex, raftStatus.truncating);
            return raftStatus.logForceFinishCondition.await(10 * 1000,
                    v -> waitWriteFinish(raftStatus, resumePoint));
        } else {
            return Fiber.resume(null, resumePoint);
        }
    }

    public static <T> Set<T> union(Collection<T> c1, Collection<T> c2) {
        HashSet<T> set = new HashSet<>();
        set.addAll(c1);
        set.addAll(c2);
        return set;
    }

    public static String setToStr(Set<Integer> s) {
        StringBuilder sb = new StringBuilder();
        for (int id : s) {
            sb.append(id).append(',');
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    public static Set<Integer> strToIdSet(String str) {
        if (str == null || str.isEmpty()) {
            return Set.of();
        }
        String[] arr = str.split(",");
        Set<Integer> set = new HashSet<>();
        for (String s : arr) {
            set.add(Integer.parseInt(s));
        }
        return set;
    }

    public static int parseInt(Map<String, String> loadedProps, String key, int defaultValue) {
        String value = loadedProps.get(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    public static long parseLong(Map<String, String> loadedProps, String key, long defaultValue) {
        String value = loadedProps.get(key);
        return value == null ? defaultValue : Long.parseLong(value);
    }

    public static boolean parseBoolean(Map<String, String> loadedProps, String key, boolean defaultValue) {
        String value = loadedProps.get(key);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    public static SimpleWritePacket buildRaftPingPacket(int nodeId, RaftStatusImpl raftStatus) {
        RaftPing raftPing = new RaftPing();
        raftPing.groupId = raftStatus.groupId;
        raftPing.nodeId = nodeId;
        Function<RaftMember, RaftNodeEx> mapper = m -> m.node;
        raftPing.members = RaftNode.formatServers(raftStatus.members, mapper);
        raftPing.observers = RaftNode.formatServers(raftStatus.observers, mapper);
        raftPing.preparedMembers = RaftNode.formatServers(raftStatus.preparedMembers, mapper);
        raftPing.preparedObservers = RaftNode.formatServers(raftStatus.preparedObservers, mapper);
        return new SimpleWritePacket(raftPing);
    }
}
