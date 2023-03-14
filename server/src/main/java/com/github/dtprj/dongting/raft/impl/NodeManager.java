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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class NodeManager extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(NodeManager.class);
    private final UUID uuid = UUID.randomUUID();
    private final int selfNodeId;
    private final NioClient client;
    private final RaftServerConfig config;
    private final IntObjMap<GroupComponents> groupComponentsMap;
    private final EventSource eventSource;

    private List<RaftNode> allRaftNodesOnlyForInit;
    private IntObjMap<RaftNodeEx> allNodesEx;

    private ScheduledFuture<?> scheduledFuture;

    private int currentReadyNodes;

    public NodeManager(RaftServerConfig config, List<RaftNode> allRaftNodes, NioClient client, IntObjMap<GroupComponents> groupComponentsMap) {
        this.selfNodeId = config.getNodeId();
        this.allRaftNodesOnlyForInit = allRaftNodes;
        this.client = client;
        this.config = config;
        this.groupComponentsMap = groupComponentsMap;

        groupComponentsMap.forEach((groupId, gc) -> {
            for (int nodeId : gc.getMemberManager().getNodeIdOfMembers()) {
                RaftNodeEx nodeEx = allNodesEx.get(nodeId);
                nodeEx.setUseCount(nodeEx.getUseCount() + 1);
            }
            for (int nodeId : gc.getMemberManager().getNodeIdOfObservers()) {
                RaftNodeEx nodeEx = allNodesEx.get(nodeId);
                nodeEx.setUseCount(nodeEx.getUseCount() + 1);
            }
            return true;
        });

        this.eventSource = new EventSource(RaftUtil.SCHEDULED_SERVICE);
    }

    private CompletableFuture<RaftNodeEx> addToNioClient(RaftNode node) {
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
        eventSource.fireInExecutorThread();
    }

    private CompletableFuture<Void> sendNodePing(RaftNodeEx nodeEx) {
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame> f = client.sendRequest(nodeEx.getPeer(),
                new NodePingWriteFrame(selfNodeId, uuid), NodePingProcessor.DECODER, timeout);
        return f.thenAccept(rf -> whenRpcFinish(rf, nodeEx));
    }

    // run in io thread
    private void whenRpcFinish(ReadFrame rf, RaftNodeEx nodeEx) {
        NodePingCallback callback = (NodePingCallback) rf.getBody();
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

    public void waitReady(int targetReadyCount) {
        try {
            CompletableFuture<Void> f = eventSource.registerInOtherThreads(() -> currentReadyNodes >= targetReadyCount);
            f.get();
        } catch (Exception e) {
            throw new RaftException(e);
        }
    }

    public CompletableFuture<RaftNodeEx> addNode(RaftNode node) {
        CompletableFuture<RaftNodeEx> f = addToNioClient(node);
        f = f.thenComposeAsync(nodeEx -> {
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
        }, RaftUtil.SCHEDULED_SERVICE);
        return f;
    }

    public CompletableFuture<Void> removeNode(int nodeId) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        Runnable r = () -> {
            // find should run in schedule thread
            RaftNodeEx existNode = allNodesEx.get(nodeId);
            if (existNode == null) {
                f.complete(null);
            } else {
                if (existNode.getUseCount() == 0) {
                    client.removePeer(existNode.getPeer()).thenRun(() -> f.complete(null));
                } else {
                    f.completeExceptionally(new RaftException("node is using"));
                }
            }
        };
        RaftUtil.SCHEDULED_SERVICE.submit(r);
        return f;
    }

    public UUID getUuid() {
        return uuid;
    }
}
