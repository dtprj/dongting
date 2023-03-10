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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.raft.rpc.RaftPingFrameCallback;
import com.github.dtprj.dongting.raft.rpc.RaftPingProcessor;
import com.github.dtprj.dongting.raft.rpc.RaftPingWriteFrame;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class MemberManager {
    private static final DtLog log = DtLogs.getLogger(MemberManager.class);
    private final RaftServerConfig serverConfig;
    private final int groupId;
    private final Set<Integer> nodeIdOfMembers;
    private final NioClient client;
    private final Executor executor;

    private final Set<Integer> nodeIdOfLearners;

    private final List<RaftMember> allMembers;
    private final List<RaftMember> learners;

    private final EventSource<Integer> eventSource;

    private int readyCount;

    public MemberManager(RaftServerConfig serverConfig, NioClient client, Executor executor,
                         RaftStatus raftStatus, int groupId, Set<Integer> nodeIdOfMembers,
                         Set<Integer> nodeIdOfLearners) {
        this.serverConfig = serverConfig;
        this.client = client;
        this.executor = executor;
        this.groupId = groupId;
        this.nodeIdOfMembers = nodeIdOfMembers;
        this.nodeIdOfLearners = nodeIdOfLearners;
        this.allMembers = raftStatus.getAllMembers();
        this.learners = raftStatus.getLearners();

        this.eventSource = new EventSource<>(executor);
    }


    public void init(IntObjMap<RaftNodeEx> allNodes) {
        for (int nodeId : nodeIdOfMembers) {
            RaftNodeEx node = allNodes.get(nodeId);
            allMembers.add(new RaftMember(node));
        }
        for (int nodeId : nodeIdOfLearners) {
            RaftNodeEx node = allNodes.get(nodeId);
            learners.add(new RaftMember(node));
        }
    }

    public void ensureRaftMemberStatus() {
        for (RaftMember member : allMembers) {
            check(member);
        }
        for (RaftMember member : learners) {
            check(member);
        }
    }

    private void check(RaftMember member) {
        RaftNodeEx node = member.getNode();
        NodeStatus nodeStatus = node.getStatus();
        if (!nodeStatus.isReady()) {
            setReady(member, false);
        } else if (nodeStatus.getEpoch() != member.getEpoch()) {
            setReady(member, false);
            if (!member.isPinging()) {
                raftPing(node, member, nodeStatus.getEpoch());
            }
        }
    }

    private void raftPing(RaftNodeEx raftNodeEx, RaftMember member, int nodeEpochWhenStartPing) {
        if (raftNodeEx.getPeer().getStatus() != PeerStatus.connected) {
            setReady(member, false);
            return;
        }

        member.setPinging(true);
        DtTime timeout = new DtTime(serverConfig.getRpcTimeout(), TimeUnit.MILLISECONDS);
        RaftPingWriteFrame f = new RaftPingWriteFrame(groupId, serverConfig.getNodeId(), nodeIdOfMembers);
        client.sendRequest(raftNodeEx.getPeer(), f, RaftPingProcessor.DECODER, timeout)
                .whenCompleteAsync((rf, ex) -> processPingResult(raftNodeEx, member, rf, ex, nodeEpochWhenStartPing), executor);
    }

    private void processPingResult(RaftNodeEx raftNodeEx, RaftMember member,
                                   ReadFrame rf, Throwable ex, int nodeEpochWhenStartPing) {
        RaftPingFrameCallback callback = (RaftPingFrameCallback) rf.getBody();
        member.setPinging(false);
        if (ex != null) {
            log.warn("raft ping fail, remote={}", raftNodeEx.getHostPort(), ex);
            setReady(member, false);
        } else {
            if (callback.nodeId == 0 && callback.groupId == 0) {
                log.error("raft ping error, group not found, groupId={}, remote={}",
                        groupId, raftNodeEx.getHostPort());
                setReady(member, false);
            } else if (nodeIdOfMembers.equals(callback.nodeIdOfMembers)) {
                NodeStatus currentNodeStatus = member.getNode().getStatus();
                if (currentNodeStatus.isReady() && nodeEpochWhenStartPing == currentNodeStatus.getEpoch()) {
                    log.info("raft ping success, id={}, remote={}", callback.nodeId, raftNodeEx.getHostPort());
                    setReady(member, true);
                    member.setEpoch(nodeEpochWhenStartPing);
                } else {
                    log.warn("raft ping success but current node status not match. "
                                    + "id={}, remoteHost={}, nodeReady={}, nodeEpoch={}, pingEpoch={}",
                            callback.nodeId, raftNodeEx.getHostPort(), currentNodeStatus.isReady(),
                            currentNodeStatus.getEpoch(), nodeEpochWhenStartPing);
                    setReady(member, false);
                }
            } else {
                log.error("raft ping error, group ids not match: localIds={}, remoteIds={}, remote={}",
                        nodeIdOfMembers, callback.nodeIdOfMembers, raftNodeEx.getHostPort());
                setReady(member, false);
            }
        }
    }

    public void setReady(RaftMember member, boolean ready) {
        if (ready == member.isReady()) {
            return;
        }
        member.setReady(ready);
        if (ready) {
            readyCount++;
        } else {
            readyCount--;
        }
        eventSource.fireInExecutorThread();
    }

    public Set<Integer> getNodeIdOfMembers() {
        return nodeIdOfMembers;
    }

    public CompletableFuture<Void> createReadyFuture(int targetReadyCount) {
        return eventSource.registerInOtherThreads(() -> readyCount >= targetReadyCount);
    }
}
