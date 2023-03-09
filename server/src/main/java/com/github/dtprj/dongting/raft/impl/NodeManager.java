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
    private final CompletableFuture<Void> nodeReadyFuture = new CompletableFuture<>();

    private List<RaftNode> allRaftNodesOnlyForInit;
    private ArrayList<RaftNodeEx> allNodesEx;

    private ScheduledFuture<?> scheduledFuture;

    private int currentReadyNodes;

    public NodeManager(RaftServerConfig config, List<RaftNode> allRaftNodes, NioClient client) {
        this.selfNodeId = config.getNodeId();
        this.allRaftNodesOnlyForInit = allRaftNodes;
        this.client = client;
        this.config = config;
    }

    private CompletableFuture<RaftNodeEx> add(RaftNode node) {
        return client.addPeer(node.getHostPort()).thenApply(peer
                -> new RaftNodeEx(node.getNodeId(), node.getHostPort(), node.isSelf(), peer));
    }

    @Override
    protected void doStart() {
        init();
        this.scheduledFuture = RaftUtil.SCHEDULED_SERVICE.scheduleWithFixedDelay(
                this::tryConnectAndPingAll, 5, 2, TimeUnit.SECONDS);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    private void init() {
        ArrayList<CompletableFuture<RaftNodeEx>> futures = new ArrayList<>();
        for (RaftNode n : allRaftNodesOnlyForInit) {
            futures.add(add(n));
        }
        allRaftNodesOnlyForInit = null;
        allNodesEx = new ArrayList<>();

        for (CompletableFuture<RaftNodeEx> f : futures) {
            RaftNodeEx node = f.join();
            if (node.isSelf()) {
                if (config.isCheckSelf()) {
                    doCheckSelf(node);
                }
            }
            allNodesEx.add(f.join());
        }

        for (RaftNodeEx nodeEx : allNodesEx) {
            if (!nodeEx.isSelf()) {
                connectAndPing(nodeEx);
            }
        }
    }

    private void doCheckSelf(RaftNodeEx nodeEx) {
        try {
            CompletableFuture<Boolean> f = connectAndPing(nodeEx);
            boolean result = f.get(config.getConnectTimeout() + config.getRpcTimeout(), TimeUnit.MILLISECONDS);
            if (!result) {
                throw new RaftException("self node ping result is false");
            }
        } catch (Exception e) {
            throw new RaftException(e);
        } finally {
            if (nodeEx.getPeer().getStatus() == PeerStatus.connected) {
                client.disconnect(nodeEx.getPeer());
            }
        }
    }

    private void tryConnectAndPingAll() {
        for (RaftNodeEx nodeEx : allNodesEx) {
            if (!nodeEx.isSelf() && !nodeEx.isConnecting()) {
                connectAndPing(nodeEx);
            }
        }
    }

    private CompletableFuture<Boolean> connectAndPing(RaftNodeEx nodeEx) {
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
            return CompletableFuture.completedFuture(null);
        }
        return connectFuture.thenCompose(v -> sendNodePing(nodeEx))
                // we should set connecting status in schedule thread
                .handleAsync((v, ex) -> processResult(nodeEx, v, ex), RaftUtil.SCHEDULED_SERVICE);
    }

    private boolean processResult(RaftNodeEx nodeEx, boolean result, Throwable ex) {
        nodeEx.setConnecting(false);
        if (ex != null) {
            log.warn("connect to raft server {} fail: {}",
                    nodeEx.getPeer().getEndPoint(), ex.toString());
            updateNodeStatus(nodeEx, false);
            return false;
        } else {
            updateNodeStatus(nodeEx, result);
            return result;
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
        RaftUtil.onReadyStatusChange(currentReadyNodes, nodeReadyFuture, RaftUtil.getElectQuorum(allNodesEx.size()));
    }

    private CompletableFuture<Boolean> sendNodePing(RaftNodeEx nodeEx) {
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame> f = client.sendRequest(nodeEx.getPeer(),
                new NodePingWriteFrame(selfNodeId, uuid), NodePingProcessor.DECODER, timeout);
        return f.handle((rf, ex) -> {
            if (ex == null) {
                return whenRpcFinish(rf, nodeEx);
            } else {
                log.warn("node ping fail. {}, {}", nodeEx.getPeer().getEndPoint(), ex.getMessage());
                return false;
            }
        });
    }

    // run in io thread
    private boolean whenRpcFinish(ReadFrame rf, RaftNodeEx nodeEx) {
        NodePingCallback callback = (NodePingCallback) rf.getBody();
        if (nodeEx.getNodeId() != callback.nodeId) {
            log.error("config fail: node id not match. expect {}, but {}", nodeEx.getNodeId(), callback.nodeId);
            return false;
        }
        boolean uuidMatch = uuid.getMostSignificantBits() == callback.uuidHigh &&
                uuid.getLeastSignificantBits() == callback.uuidLow;
        if (nodeEx.isSelf()) {
            if (!uuidMatch) {
                log.error("config fail: self node uuid not match");
                return false;
            }
        } else {
            if (uuidMatch) {
                log.error("config fail: node uuid match");
                return false;
            }
        }
        return true;
    }

    public ArrayList<RaftNodeEx> getAllNodesEx() {
        return allNodesEx;
    }

    public void waitReady() {
        try {
            nodeReadyFuture.get();
        } catch (Exception e) {
            log.error("error during wait node ready", e);
            throw new RaftException(e);
        }
    }

    public UUID getUuid() {
        return uuid;
    }
}
