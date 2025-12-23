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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.net.WritePacket;
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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public class NodeManager extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(NodeManager.class);
    private final ReentrantLock lock = new ReentrantLock();
    private final UUID uuid = UUID.randomUUID();
    private final int selfNodeId;
    private final NioClient client;
    private final RaftServerConfig config;
    private final NioServer nioServer;
    Executor executor;

    // update by RaftServer init thread and schedule thread
    final IntObjMap<RaftNodeEx> allNodesEx;

    private List<RaftNode> allRaftNodesOnlyForInit;

    int currentReadyNodes;

    private final CompletableFuture<Void> nodePingReadyFuture = new CompletableFuture<>();
    private boolean nodePingReady = false;
    private final int startReadyQuorum;

    private final long pingIntervalMillis;

    public NodeManager(RaftServerConfig config, List<RaftNode> allRaftNodes,
                       NioClient client, int startReadyQuorum, NioServer nioServer) {
        this.selfNodeId = config.nodeId;
        this.client = client;
        this.config = config;
        this.startReadyQuorum = startReadyQuorum;
        this.pingIntervalMillis = config.pingInterval;

        this.allNodesEx = new IntObjMap<>(allRaftNodes.size() * 2, 0.75f);
        this.allRaftNodesOnlyForInit = allRaftNodes;
        this.nioServer = nioServer;

        if (allRaftNodes.stream().noneMatch(n -> n.nodeId == selfNodeId)) {
            throw new IllegalArgumentException("self node id not exist in servers: " + selfNodeId);
        }
    }

    private CompletableFuture<RaftNodeEx> addToNioClient(RaftNode node) {
        boolean self = node.nodeId == selfNodeId;
        return client.addPeer(node.hostPort).thenApply(peer
                -> new RaftNodeEx(node.nodeId, node.hostPort, self, peer));
    }

    @Override
    protected void doStart() {
        submitPingAllTask();
    }

    private void submitPingAllTask() {
        executor.execute(this::runPingAllTask);
    }

    private void runPingAllTask() {
        try {
            if (status == STATUS_RUNNING) {
                tryNodePingAll();
            }
        } finally {
            if (status == STATUS_RUNNING) {
                DtUtil.SCHEDULED_SERVICE.schedule(this::submitPingAllTask, pingIntervalMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
    }

    public void initNodes(ConcurrentHashMap<Integer, RaftGroupImpl> raftGroups) {
        CompletableFuture<Void> selfCheckFuture = null;
        RaftNodeEx selfNodeEx = null;
        lock.lock();
        try {
            this.executor = nioServer.getBizExecutor();
            ArrayList<CompletableFuture<RaftNodeEx>> futures = new ArrayList<>();
            for (RaftNode n : allRaftNodesOnlyForInit) {
                futures.add(addToNioClient(n));
            }
            allRaftNodesOnlyForInit = null;

            for (CompletableFuture<RaftNodeEx> f : futures) {
                RaftNodeEx nodeEx = f.join();
                allNodesEx.put(nodeEx.nodeId, nodeEx);
                if (nodeEx.self && config.checkSelf) {
                    selfNodeEx = nodeEx;
                    selfCheckFuture = nodePing(nodeEx);
                }
            }
        } finally {
            lock.unlock();
        }

        // check self
        try {
            if (selfCheckFuture != null) {
                selfCheckFuture.get(config.connectTimeout + config.rpcTimeout, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            throw new RaftException(e);
        } finally {
            if (selfNodeEx!= null && selfNodeEx.peer.status == PeerStatus.connected) {
                client.disconnect(selfNodeEx.peer);
            }
        }

        lock.lock();
        try {
            raftGroups.forEach((groupId, g) -> processUseCountForGroupInLock(g, true));
        } finally {
            lock.unlock();
        }
    }

    private void tryNodePingAll() {
        if (status < STATUS_PREPARE_STOP) {
            lock.lock();
            try {
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
            } finally {
                lock.unlock();
            }
        }
    }

    private CompletableFuture<Void> nodePing(RaftNodeEx nodeEx) {
        nodeEx.pinging = true;

        DtTime timeout = new DtTime(config.rpcTimeout, TimeUnit.MILLISECONDS);
        SimpleWritePacket packet = new SimpleWritePacket(new NodePing(selfNodeId, nodeEx.nodeId, uuid));
        packet.command = Commands.NODE_PING;
        CompletableFuture<ReadPacket<NodePing>> f = new CompletableFuture<>();
        client.sendRequest(nodeEx.peer, packet, ctx -> ctx.toDecoderCallback(new NodePing()),
                timeout, RpcCallback.fromFuture(f));
        CompletableFuture<Void> f2 = f.thenAccept(rf -> whenRpcFinish(rf, nodeEx));
        // we should set connecting status in schedule thread
        return f2.whenCompleteAsync((v, ex) ->
                processResultInLock(nodeEx, ex), executor);
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

    private void processResultInLock(RaftNodeEx nodeEx, Throwable ex) {
        lock.lock();
        try {
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
        } finally {
            lock.unlock();
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
        if (!nodePingReady && currentReadyNodes >= startReadyQuorum && !nodePingReadyFuture.isDone()) {
            log.info("nodeManager is ready");
            nodePingReadyFuture.complete(null);
            nodePingReady = true;
        }
    }

    public void checkLeaderPrepare(Set<Integer> memberIds, Set<Integer> observerIds) {
        lock.lock();
        try {
            checkNodeIdSet(memberIds);
            checkNodeIdSet(observerIds);
        } finally {
            lock.unlock();
        }
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

    public List<List<RaftNodeEx>> doApplyConfig(Set<Integer> oldMemberIds, Set<Integer> oldObserverIds,
                                                Set<Integer> oldPreparedMemberIds, Set<Integer> oldPreparedObserverIds,
                                                Set<Integer> newMemberIds, Set<Integer> newObserverIds,
                                                Set<Integer> newPreparedMemberIds, Set<Integer> newPreparedObserverIds) {
        lock.lock();
        try {
            checkNodeIdSet(oldMemberIds);
            checkNodeIdSet(oldObserverIds);
            checkNodeIdSet(oldPreparedMemberIds);
            checkNodeIdSet(oldPreparedObserverIds);

            List<RaftNodeEx> newMembers = checkNodeIdSet(newMemberIds);
            List<RaftNodeEx> newObservers = checkNodeIdSet(newObserverIds);
            List<RaftNodeEx> newPreparedMembers = checkNodeIdSet(newPreparedMemberIds);
            List<RaftNodeEx> newPreparedObservers = checkNodeIdSet(newPreparedObserverIds);

            processUseCountInLock(newMemberIds, 1);
            processUseCountInLock(newObserverIds, 1);
            processUseCountInLock(newPreparedMemberIds, 1);
            processUseCountInLock(newPreparedObserverIds, 1);

            processUseCountInLock(oldMemberIds, -1);
            processUseCountInLock(oldObserverIds, -1);
            processUseCountInLock(oldPreparedMemberIds, -1);
            processUseCountInLock(oldPreparedObserverIds, -1);
            return List.of(newMembers, newObservers, newPreparedMembers, newPreparedObservers);
        } finally {
            lock.unlock();
        }
    }

    public void processUseCountForGroupInLock(RaftGroupImpl g, boolean add) {
        RaftStatusImpl raftStatus = g.groupComponents.raftStatus;
        int delta = add ? 1 : -1;
        processUseCountInLock(raftStatus.nodeIdOfMembers, delta);
        processUseCountInLock(raftStatus.nodeIdOfObservers, delta);
        processUseCountInLock(raftStatus.nodeIdOfPreparedMembers, delta);
        processUseCountInLock(raftStatus.nodeIdOfPreparedObservers, delta);
    }

    private void processUseCountInLock(Collection<Integer> nodeIds, int delta) {
        if (nodeIds == null) {
            return;
        }
        for (int nodeId : nodeIds) {
            RaftNodeEx nodeEx = allNodesEx.get(nodeId);
            nodeEx.useCount = nodeEx.useCount + delta;
        }
    }

    public CompletableFuture<RaftNodeEx> addNode(RaftNode node) {
        CompletableFuture<RaftNodeEx> f = new CompletableFuture<>();
        addToNioClient(node).whenCompleteAsync((nodeEx, ex) -> {
            lock.lock();
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
            } finally {
                lock.unlock();
            }
        }, executor);
        return f;
    }

    public CompletableFuture<Void> removeNode(int nodeId) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        if (nodeId == selfNodeId) {
            f.completeExceptionally(new RaftException("can not remove self"));
            return f;
        }
        lock.lock();
        try {
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
        } finally {
            lock.unlock();
        }
        return f;
    }

    public CompletableFuture<Void> getNodePingReadyFuture() {
        return nodePingReadyFuture;
    }

    public UUID getUuid() {
        return uuid;
    }

    // create new set since this method invoke occasionally
    public Set<Integer> getAllNodeIdsInLock() {
        HashSet<Integer> ids = new HashSet<>();
        allNodesEx.forEach((nodeId, nodeEx) -> {
            ids.add(nodeId);
        });
        return ids;
    }

    public void processNodePing(ReadPacket<NodePing> packet, ReqContext reqContext) {
        lock.lock();
        try {
            NodePing reqPing = packet.getBody();
            WritePacket p;
            if (allNodesEx.get(reqPing.localNodeId) == null) {
                p = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
                p.msg = "node not found: " + reqPing.localNodeId;
            } else {
                NodePing respPing = new NodePing(selfNodeId, reqPing.localNodeId, uuid);
                p = new SimpleWritePacket(respPing);
                p.respCode = CmdCodes.SUCCESS;
            }
            reqContext.writeRespInBizThreads(p);
        } finally {
            lock.unlock();
        }
    }

    public ReentrantLock getLock() {
        return lock;
    }
}
