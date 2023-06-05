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

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.rpc.NodePingCallback;
import com.github.dtprj.dongting.raft.rpc.NodePingProcessor;
import com.github.dtprj.dongting.raft.rpc.NodePingWriteFrame;
import com.github.dtprj.dongting.raft.server.RaftNode;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * @author huangli
 */
public class NodeManager extends AbstractLifeCircle implements BiConsumer<EventType, Object> {
    private static final DtLog log = DtLogs.getLogger(NodeManager.class);
    private final UUID uuid = UUID.randomUUID();
    private final int selfNodeId;
    private final NioClient client;
    private final RaftServerConfig config;
    private final RaftGroups raftGroups;
    private final FutureEventSource futureEventSource;

    private List<RaftNode> allRaftNodesOnlyForInit;
    private IntObjMap<RaftNodeEx> allNodesEx;

    private ScheduledFuture<?> scheduledFuture;

    private int currentReadyNodes;

    public NodeManager(RaftServerConfig config, List<RaftNode> allRaftNodes, NioClient client,
                       RaftGroups raftGroups) {
        this.selfNodeId = config.getNodeId();
        this.allRaftNodesOnlyForInit = allRaftNodes;
        this.client = client;
        this.config = config;
        this.raftGroups = raftGroups;

        raftGroups.forEach((groupId, gc) -> {
            for (int nodeId : gc.getRaftStatus().getNodeIdOfMembers()) {
                RaftNodeEx nodeEx = allNodesEx.get(nodeId);
                nodeEx.setUseCount(nodeEx.getUseCount() + 1);
            }
            for (int nodeId : gc.getRaftStatus().getNodeIdOfObservers()) {
                RaftNodeEx nodeEx = allNodesEx.get(nodeId);
                nodeEx.setUseCount(nodeEx.getUseCount() + 1);
            }
            return true;
        });

        this.futureEventSource = new FutureEventSource(RaftUtil.SCHEDULED_SERVICE);
    }

    /**
     * run in raft thread.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void accept(EventType eventType, Object o) {
        RaftUtil.SCHEDULED_SERVICE.execute(() -> {
            if (eventType == EventType.prepareConfChange) {
                doPrepare((Object[]) o);
            } else if (eventType == EventType.abortConfChange) {
                doAbort((Set<Integer>) o);
            } else if (eventType == EventType.commitConfChange) {
                doCommit((Set<Integer>) o);
            }
        });
    }

    public CompletableFuture<RaftNodeEx> addToNioClient(RaftNode node) {
        return client.addPeer(node.getHostPort()).thenApply(peer
                -> new RaftNodeEx(node.getNodeId(), node.getHostPort(), node.isSelf(), peer));
    }

    @Override
    protected void doStart() {
        initNodes();
        this.scheduledFuture = RaftUtil.SCHEDULED_SERVICE.scheduleWithFixedDelay(
                this::tryConnectAndPingAll, 5, 2, TimeUnit.SECONDS);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    private void initNodes() {
        ArrayList<CompletableFuture<RaftNodeEx>> futures = new ArrayList<>();
        for (RaftNode n : allRaftNodesOnlyForInit) {
            futures.add(addToNioClient(n));
        }
        allRaftNodesOnlyForInit = null;
        allNodesEx = new IntObjMap<>(futures.size() * 2, 0.75f);

        for (CompletableFuture<RaftNodeEx> f : futures) {
            RaftNodeEx node = f.join();
            if (node.isSelf()) {
                if (config.isCheckSelf()) {
                    doCheckSelf(node);
                }
            }
            RaftNodeEx nodeEx = f.join();
            allNodesEx.put(nodeEx.getNodeId(), nodeEx);
        }

        allNodesEx.forEach((nodeId, nodeEx) -> {
            if (!nodeEx.isSelf()) {
                connectAndPing(nodeEx);
            }
            return true;
        });
    }

    private void doCheckSelf(RaftNodeEx nodeEx) {
        try {
            CompletableFuture<Void> f = connectAndPing(nodeEx);
            f.get(config.getConnectTimeout() + config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new RaftException(e);
        } finally {
            if (nodeEx.getPeer().getStatus() == PeerStatus.connected) {
                client.disconnect(nodeEx.getPeer());
            }
        }
    }

    private void tryConnectAndPingAll() {
        allNodesEx.forEach((nodeId, nodeEx) -> {
            if (!nodeEx.isSelf() && !nodeEx.isConnecting()) {
                connectAndPing(nodeEx);
            }
            return true;
        });
    }

    private CompletableFuture<Void> connectAndPing(RaftNodeEx nodeEx) {
        nodeEx.setConnecting(true);
        CompletableFuture<Void> connectFuture;
        PeerStatus peerStatus = nodeEx.getPeer().getStatus();
        if (peerStatus == PeerStatus.connected) {
            connectFuture = CompletableFuture.completedFuture(null);
        } else if (peerStatus == PeerStatus.not_connect) {
            DtTime deadline = new DtTime(config.getConnectTimeout(), TimeUnit.MILLISECONDS);
            connectFuture = client.connect(nodeEx.getPeer(), deadline);
        } else {
            BugLog.getLog().error("assert false, peer status is connecting");
            connectFuture = CompletableFuture.failedFuture(new RaftException("peer status is connecting"));
        }
        return connectFuture.thenCompose(v -> sendNodePing(nodeEx))
                // we should set connecting status in schedule thread
                .whenCompleteAsync((v, ex) -> processResult(nodeEx, ex), RaftUtil.SCHEDULED_SERVICE);
    }

    private void processResult(RaftNodeEx nodeEx, Throwable ex) {
        nodeEx.setConnecting(false);
        if (ex != null) {
            log.warn("ping raft node {} fail: {}",
                    nodeEx.getPeer().getEndPoint(), ex.toString());
            updateNodeStatus(nodeEx, false);
        } else {
            updateNodeStatus(nodeEx, true);
        }
    }

    private void updateNodeStatus(RaftNodeEx nodeEx, boolean ready) {
        NodeStatus oldStatus = nodeEx.getStatus();
        if (ready == oldStatus.isReady()) {
            return;
        }
        if (ready) {
            currentReadyNodes++;
            nodeEx.setStatus(new NodeStatus(true, oldStatus.getEpoch() + 1));
        } else {
            currentReadyNodes--;
            nodeEx.setStatus(new NodeStatus(false, oldStatus.getEpoch()));
        }
        futureEventSource.fireInExecutorThread();
    }

    private CompletableFuture<Void> sendNodePing(RaftNodeEx nodeEx) {
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame<NodePingCallback>> f = client.sendRequest(nodeEx.getPeer(),
                new NodePingWriteFrame(selfNodeId, uuid), NodePingProcessor.DECODER, timeout);
        return f.thenAccept(rf -> whenRpcFinish(rf, nodeEx));
    }

    // run in io thread
    private void whenRpcFinish(ReadFrame<NodePingCallback> rf, RaftNodeEx nodeEx) {
        NodePingCallback callback = rf.getBody();
        if (nodeEx.getNodeId() != callback.nodeId) {
            String msg = "config fail: node id not match. expect " + nodeEx.getNodeId() + ", but " + callback.nodeId;
            log.error(msg);
            throw new RaftException(msg);
        }
        boolean uuidMatch = uuid.getMostSignificantBits() == callback.uuidHigh &&
                uuid.getLeastSignificantBits() == callback.uuidLow;
        if (nodeEx.isSelf()) {
            if (!uuidMatch) {
                String msg = "config fail: self node uuid not match";
                log.error(msg);
                throw new RaftException(msg);
            }
        } else {
            if (uuidMatch) {
                String msg = "config fail: node uuid match";
                log.error(msg);
                throw new RaftException(msg);
            }
        }
    }

    public IntObjMap<RaftNodeEx> getAllNodesEx() {
        return allNodesEx;
    }

    // create new set since this method invoke occasionally
    public Set<Integer> getAllNodeIds() {
        HashSet<Integer> ids = new HashSet<>();
        allNodesEx.forEach((nodeId, nodeEx) -> {
            ids.add(nodeId);
            return true;
        });
        return ids;
    }

    public void waitReady(int targetReadyCount) {
        try {
            CompletableFuture<Void> f = futureEventSource.registerInOtherThreads(() -> currentReadyNodes >= targetReadyCount);
            f.get();
        } catch (Exception e) {
            throw new RaftException(e);
        }
    }

    public CompletableFuture<RaftNodeEx> addNode(RaftNodeEx nodeEx) {
        RaftNodeEx existNode = allNodesEx.get(nodeEx.getNodeId());
        if (existNode != null) {
            return CompletableFuture.completedFuture(existNode);
        } else {
            CompletableFuture<Void> pingFuture = connectAndPing(nodeEx);
            return pingFuture.handleAsync((v, ex) -> {
                if (ex == null) {
                    allNodesEx.put(nodeEx.getNodeId(), nodeEx);
                    processResult(nodeEx, null);
                    return nodeEx;
                } else {
                    log.error("add node {} fail", nodeEx.getPeer().getEndPoint(), ex);
                    throw new RaftException(ex);
                }
            }, RaftUtil.SCHEDULED_SERVICE);
        }
    }

    public void removeNode(int nodeId, CompletableFuture<Void> f) {
        RaftNodeEx existNode = allNodesEx.get(nodeId);
        if (existNode == null) {
            f.complete(null);
        } else {
            if (existNode.getUseCount() == 0) {
                client.removePeer(existNode.getPeer()).thenRun(() -> f.complete(null));
            } else {
                f.completeExceptionally(new RaftException("node is using, current ref count: " + existNode.getUseCount()));
            }
        }
    }

    public void leaderPrepareJointConsensus(CompletableFuture<Void> f, RaftGroupImpl raftGroup,
                                            Set<Integer> memberIds, Set<Integer> observerIds) {
        try {
            int groupId = raftGroup.getGroupId();
            for (int nodeId : memberIds) {
                if (observerIds.contains(nodeId)) {
                    log.error("node is both member and observer: nodeId={}, groupId={}", nodeId, groupId);
                    f.completeExceptionally(new RaftException("node is both member and observer: " + nodeId));
                    return;
                }
            }
            if (raftGroup.getRaftStatus().isStop()) {
                f.completeExceptionally(new RaftException("group is stopped"));
                return;
            }
            checkNodeIdSet(groupId, memberIds);
            checkNodeIdSet(groupId, observerIds);
            raftGroup.getRaftExecutor().execute(() -> raftGroup.getMemberManager()
                    .leaderPrepareJointConsensus(memberIds, observerIds, f));
        } catch (Throwable e) {
            f.completeExceptionally(e);
        }
    }

    private List<RaftNodeEx> checkNodeIdSet(int groupId, Set<Integer> nodeIds) {
        List<RaftNodeEx> memberNodes = new ArrayList<>(nodeIds.size());
        for (Integer nodeId : nodeIds) {
            if (allNodesEx.get(nodeId) == null) {
                log.error("node not exist: nodeId={}, groupId={}", nodeId, groupId);
                throw new RaftException("node not exist: " + nodeId);
            } else {
                memberNodes.add(allNodesEx.get(nodeId));
            }
        }
        return memberNodes;
    }

    @SuppressWarnings("unchecked")
    private void doPrepare(Object[] args) {
        RaftGroupImpl gc = null;
        Runnable callback = (Runnable) args[5];
        try {
            int groupId = (Integer) args[0];
            Set<Integer> oldPrepareMembers = (Set<Integer>) args[1];
            Set<Integer> oldPrepareObservers = (Set<Integer>) args[2];
            Set<Integer> newMembers = (Set<Integer>) args[3];
            Set<Integer> newObservers = (Set<Integer>) args[4];

            gc = RaftUtil.getGroupComponents(raftGroups, groupId);
            List<RaftNodeEx> newMemberNodes = checkNodeIdSet(groupId, newMembers);
            List<RaftNodeEx> newObserverNodes = checkNodeIdSet(groupId, newObservers);
            processUseCount(oldPrepareMembers, -1);
            processUseCount(oldPrepareObservers, -1);
            processUseCount(newMembers, 1);
            processUseCount(newObservers, 1);
            MemberManager memberManager = gc.getMemberManager();
            gc.getRaftExecutor().execute(() -> memberManager.doPrepare(newMemberNodes, newObserverNodes, null, callback));
        } catch (Throwable e) {
            log.error("prepare fail", e);
            if (gc != null) {
                MemberManager memberManager = gc.getMemberManager();
                gc.getRaftExecutor().execute(() -> memberManager.doPrepare(
                        null, null, e, callback));
            }
        }
    }

    private void processUseCount(Collection<Integer> nodeIds, int delta) {
        for (int nodeId : nodeIds) {
            RaftNodeEx nodeEx = allNodesEx.get(nodeId);
            if (nodeEx != null) {
                nodeEx.setUseCount(nodeEx.getUseCount() + delta);
            } else {
                log.error("node not exist: nodeId={}", nodeId);
            }
        }
    }

    private void doAbort(Set<Integer> ids) {
        processUseCount(ids, -1);
    }

    private void doCommit(Set<Integer> ids) {
        processUseCount(ids, -1);
    }

    public UUID getUuid() {
        return uuid;
    }
}
