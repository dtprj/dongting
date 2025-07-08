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
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.rpc.NodePing;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class NodeManager extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(NodeManager.class);
    private final UUID uuid = UUID.randomUUID();
    private final int selfNodeId;
    private final NioClient client;
    private final RaftServerConfig config;

    // update by RaftServer init thread and schedule thread
    final IntObjMap<RaftNodeEx> allNodesEx;

    private List<RaftNode> allRaftNodesOnlyForInit;

    private ScheduledFuture<?> scheduledFuture;

    int currentReadyNodes;

    private final CompletableFuture<Void> nodePingReadyFuture = new CompletableFuture<>();
    private final int startReadyQuorum;

    int pingIntervalMillis = 2000;

    public NodeManager(RaftServerConfig config, List<RaftNode> allRaftNodes, NioClient client, int startReadyQuorum) {
        this.selfNodeId = config.nodeId;
        this.client = client;
        this.config = config;
        this.startReadyQuorum = startReadyQuorum;

        this.allNodesEx = new IntObjMap<>(allRaftNodes.size() * 2, 0.75f);
        this.allRaftNodesOnlyForInit = allRaftNodes;
    }

    private CompletableFuture<RaftNodeEx> addToNioClient(RaftNode node) {
        boolean self = node.nodeId == selfNodeId;
        return client.addPeer(node.hostPort).thenApply(peer
                -> new RaftNodeEx(node.nodeId, node.hostPort, self, peer));
    }

    @Override
    protected void doStart() {
        this.scheduledFuture = DtUtil.SCHEDULED_SERVICE.scheduleWithFixedDelay(
                this::tryNodePingAll, 0, pingIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    public void initNodes(ConcurrentHashMap<Integer, RaftGroupImpl> raftGroups) {
        ArrayList<CompletableFuture<RaftNodeEx>> futures = new ArrayList<>();
        for (RaftNode n : allRaftNodesOnlyForInit) {
            futures.add(addToNioClient(n));
        }
        allRaftNodesOnlyForInit = null;

        for (CompletableFuture<RaftNodeEx> f : futures) {
            RaftNodeEx nodeEx = f.join();
            allNodesEx.put(nodeEx.nodeId, nodeEx);
            if (nodeEx.self && config.checkSelf) {
                doCheckSelf(nodeEx);
            }
        }

        raftGroups.forEach((groupId, g) -> {
            RaftStatusImpl raftStatus = g.groupComponents.raftStatus;
            for (int nodeId : raftStatus.nodeIdOfMembers) {
                RaftNodeEx nodeEx = allNodesEx.get(nodeId);
                nodeEx.useCount = nodeEx.useCount + 1;
            }
            for (int nodeId : raftStatus.nodeIdOfObservers) {
                RaftNodeEx nodeEx = allNodesEx.get(nodeId);
                nodeEx.useCount = nodeEx.useCount + 1;
            }
        });
    }

    private void doCheckSelf(RaftNodeEx nodeEx) {
        try {
            CompletableFuture<Void> f = nodePing(nodeEx);
            f.get(config.connectTimeout + config.rpcTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new RaftException(e);
        } finally {
            if (nodeEx.peer.status == PeerStatus.connected) {
                client.disconnect(nodeEx.peer);
            }
        }
    }

    private void tryNodePingAll() {
        if (status < STATUS_PREPARE_STOP) {
            allNodesEx.forEach((nodeId, nodeEx) -> {
                if (!nodeEx.self && !nodeEx.pinging) {
                    try {
                        nodePing(nodeEx);
                    } catch (Throwable e) {
                        log.error("node ping error", e);
                        nodeEx.pinging = false;
                    }
                }
            });
        }
    }

    private CompletableFuture<Void> nodePing(RaftNodeEx nodeEx) {
        nodeEx.pinging = true;

        DtTime timeout = new DtTime(config.rpcTimeout, TimeUnit.MILLISECONDS);
        SimpleWritePacket packet = new SimpleWritePacket(new NodePing(selfNodeId, nodeEx.nodeId, uuid));
        packet.setCommand(Commands.NODE_PING);
        CompletableFuture<ReadPacket<NodePing>> f = new CompletableFuture<>();
        client.sendRequest(nodeEx.peer, packet, ctx -> ctx.toDecoderCallback(new NodePing()),
                timeout, RpcCallback.fromFuture(f));
        CompletableFuture<Void> f2 = f.thenAccept(rf -> whenRpcFinish(rf, nodeEx));
        // we should set connecting status in schedule thread
        return f2.whenCompleteAsync((v, ex) ->
                processResultInScheduleThread(nodeEx, ex), DtUtil.SCHEDULED_SERVICE);
    }

    // run in io thread
    private void whenRpcFinish(ReadPacket<NodePing> rf, RaftNodeEx nodeEx) {
        NodePing np = rf.getBody();
        if (nodeEx.nodeId != np.localNodeId) {
            String msg = "config fail: node id not match. expect " + nodeEx.nodeId + ", but " + np.localNodeId;
            log.error(msg);
            throw new RaftException(msg);
        }
        boolean uuidMatch = uuid.getMostSignificantBits() == np.uuidHigh &&
                uuid.getLeastSignificantBits() == np.uuidLow;
        if (nodeEx.self) {
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

    private void processResultInScheduleThread(RaftNodeEx nodeEx, Throwable ex) {
        nodeEx.pinging = false;
        if (ex != null) {
            log.error("node ping fail, localId={}, remoteId={}, endPoint={}, err={}",
                    selfNodeId, nodeEx.nodeId, nodeEx.peer.endPoint, ex.toString());
            updateNodeStatus(nodeEx, false);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("node ping success, remoteId={}, endPoint={}",
                        nodeEx.nodeId, nodeEx.peer.endPoint);
            }
            updateNodeStatus(nodeEx, true);
        }
    }

    private void updateNodeStatus(RaftNodeEx nodeEx, boolean ready) {
        NodeStatus oldStatus = nodeEx.status;
        if (ready == oldStatus.isReady()) {
            return;
        }
        if (ready) {
            currentReadyNodes++;
            nodeEx.status = new NodeStatus(true, oldStatus.getEpoch() + 1);
        } else {
            currentReadyNodes--;
            nodeEx.status = new NodeStatus(false, oldStatus.getEpoch());
        }
        if (currentReadyNodes >= startReadyQuorum && !nodePingReadyFuture.isDone()) {
            log.info("nodeManager is ready");
            nodePingReadyFuture.complete(null);
        }
    }

    public FiberFuture<Void> checkLeaderPrepare(Set<Integer> memberIds, Set<Integer> observerIds) {
        return runInScheduleThread("checkLeaderPrepare", () -> {
            checkNodeIdSet(memberIds);
            checkNodeIdSet(observerIds);
            return null;
        });
    }

    private List<RaftNodeEx> checkNodeIdSet(Set<Integer> nodeIds) {
        List<RaftNodeEx> memberNodes = new ArrayList<>(nodeIds.size());
        for (Integer nodeId : nodeIds) {
            if (allNodesEx.get(nodeId) == null) {
                log.error("node not exist: nodeId={}", nodeId);
                throw new RaftException("node not exist: " + nodeId);
            } else {
                memberNodes.add(allNodesEx.get(nodeId));
            }
        }
        return memberNodes;
    }

    private <T> FiberFuture<T> runInScheduleThread(String futureName, Supplier<T> supplier) {
        FiberFuture<T> f = FiberGroup.currentGroup().newFuture(futureName);
        DtUtil.SCHEDULED_SERVICE.execute(() -> {
            try {
                f.fireComplete(supplier.get());
            } catch (Throwable e) {
                f.fireCompleteExceptionally(e);
            }
        });
        return f;
    }

    public FiberFuture<List<List<RaftNodeEx>>> doApplyConfig(Set<Integer> oldMemberIds, Set<Integer> oldObserverIds,
                                                             Set<Integer> oldPreparedMemberIds, Set<Integer> oldPreparedObserverIds,
                                                             Set<Integer> newMemberIds, Set<Integer> newObserverIds,
                                                             Set<Integer> newPreparedMemberIds, Set<Integer> newPreparedObserverIds) {
        return runInScheduleThread("appleConfigInSchedule", () -> {
            checkNodeIdSet(oldMemberIds);
            checkNodeIdSet(oldObserverIds);
            checkNodeIdSet(oldPreparedMemberIds);
            checkNodeIdSet(oldPreparedObserverIds);

            List<RaftNodeEx> newMembers = checkNodeIdSet(newMemberIds);
            List<RaftNodeEx> newObservers = checkNodeIdSet(newObserverIds);
            List<RaftNodeEx> newPreparedMembers = checkNodeIdSet(newPreparedMemberIds);
            List<RaftNodeEx> newPreparedObservers = checkNodeIdSet(newPreparedObserverIds);

            processUseCount(newMemberIds, 1);
            processUseCount(newObserverIds, 1);
            processUseCount(newPreparedMemberIds, 1);
            processUseCount(newPreparedObserverIds, 1);

            processUseCount(oldMemberIds, -1);
            processUseCount(oldObserverIds, -1);
            processUseCount(oldPreparedMemberIds, -1);
            processUseCount(oldPreparedObserverIds, -1);
            return List.of(newMembers, newObservers, newPreparedMembers, newPreparedObservers);
        });
    }

    private void processUseCount(Collection<Integer> nodeIds, int delta) {
        for (int nodeId : nodeIds) {
            RaftNodeEx nodeEx = allNodesEx.get(nodeId);
            nodeEx.useCount = nodeEx.useCount + delta;
        }
    }

    public CompletableFuture<RaftNodeEx> addNode(RaftNode node) {
        CompletableFuture<RaftNodeEx> f = new CompletableFuture<>();
        addToNioClient(node).whenCompleteAsync((nodeEx, ex) -> {
            try {
                RaftNodeEx existNode = allNodesEx.get(nodeEx.nodeId);
                if (existNode != null) {
                    if (existNode.hostPort.equals(node.hostPort)) {
                        f.complete(existNode);
                    } else {
                        f.completeExceptionally(new RaftException("node " + node.nodeId
                                + " already exist but host/port is not same, new node is " + node.hostPort
                                + ", exist one is " + existNode.hostPort));
                    }
                } else {
                    allNodesEx.put(nodeEx.nodeId, nodeEx);
                    f.complete(nodeEx);
                }
            } catch (Exception unexpected) {
                log.error("", unexpected);
                f.completeExceptionally(unexpected);
            }
        }, DtUtil.SCHEDULED_SERVICE);
        return f;
    }

    public CompletableFuture<Void> removeNode(int nodeId) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        if (nodeId == selfNodeId) {
            f.completeExceptionally(new RaftException("can not remove self"));
            return f;
        }
        DtUtil.SCHEDULED_SERVICE.execute(() -> {
            try {
                RaftNodeEx existNode = allNodesEx.get(nodeId);
                if (existNode == null) {
                    log.warn("node {} not exist", nodeId);
                    f.complete(null);
                } else {
                    if (existNode.useCount == 0) {
                        client.removePeer(existNode.peer).thenRun(() -> f.complete(null));
                    } else {
                        f.completeExceptionally(new RaftException("node is using, current ref count: " + existNode.useCount));
                    }
                }
            } catch (Exception unexpected) {
                log.error("", unexpected);
                f.completeExceptionally(unexpected);
            }
        });
        return f;
    }

    public CompletableFuture<Void> getNodePingReadyFuture() {
        return nodePingReadyFuture;
    }

    public UUID getUuid() {
        return uuid;
    }

    // should access in schedule thread, create new set since this method invoke occasionally
    public Set<Integer> getAllNodeIds() {
        HashSet<Integer> ids = new HashSet<>();
        allNodesEx.forEach((nodeId, nodeEx) -> {
            ids.add(nodeId);
        });
        return ids;
    }

    // should access in schedule thread
    public boolean containsNode(int nodeId) {
        return allNodesEx.get(nodeId) != null;
    }
}
