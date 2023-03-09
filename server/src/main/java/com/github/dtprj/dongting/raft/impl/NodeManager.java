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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.pb.PbUtil;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.server.RaftNode;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.nio.ByteBuffer;
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
    private final List<RaftNode> allRaftNodes;
    private final NioClient client;
    private final RaftServerConfig config;
    private final CompletableFuture<Void> nodeReadyFuture = new CompletableFuture<>();

    private RaftNodeEx self;
    private ArrayList<RaftNodeEx> allNodesEx;

    private ScheduledFuture<?> scheduledFuture;

    private int currentReadyNodes;

    private static final PbZeroCopyDecoder DECODER = new PbZeroCopyDecoder(ctx -> new NodePingCallback());

    public NodeManager(RaftServerConfig config, List<RaftNode> allRaftNodes, NioClient client) {
        this.selfNodeId = config.getNodeId();
        this.allRaftNodes = allRaftNodes;
        this.client = client;
        this.config = config;
        RaftNode s = null;
        for (RaftNode node : allRaftNodes) {
            if (node.getNodeId() == selfNodeId) {
                s = node;
                break;
            }
        }
        if (s == null) {
            throw new IllegalArgumentException("self node not found");
        }
    }

    private CompletableFuture<RaftNodeEx> add(RaftNode node) {
        return client.addPeer(node.getHostPort()).thenApply(peer
                -> new RaftNodeEx(node.getNodeId(), node.getHostPort(), peer));
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
        for (RaftNode n : allRaftNodes) {
             futures.add(add(n));
        }
        allNodesEx = new ArrayList<>();

        for (CompletableFuture<RaftNodeEx> f : futures) {
            RaftNodeEx node = f.join();
            if (node.getNodeId() == selfNodeId) {
                if (config.isCheckSelf()) {
                    doCheckSelf(node);
                }
                node.setSelf(true);
                this.self = node;
            }
            allNodesEx.add(f.join());
        }

        for (RaftNodeEx nodeEx : allNodesEx) {
            if (nodeEx != self) {
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
            if (nodeEx != self && !nodeEx.isConnecting()) {
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
        CompletableFuture<ReadFrame> f = client.sendRequest(nodeEx.getPeer(), new NodePingWriteFrame(), DECODER, timeout);
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
        if (self.getNodeId() == nodeEx.getNodeId()) {
            if (uuid.getMostSignificantBits() != callback.uuidHigh || uuid.getLeastSignificantBits() != callback.uuidLow) {
                log.error("config fail: self node uuid not match");
                return false;
            }
        }
        return true;
    }

    public RaftNodeEx getSelf() {
        return self;
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

    private class NodePingWriteFrame extends WriteFrame {

        public NodePingWriteFrame() {
            setCommand(Commands.RAFT_PING);
        }

        @Override
        protected int calcEstimateBodySize() {
            return PbUtil.accurateFix32Size(1, selfNodeId)
                    + PbUtil.accurateFix64Size(2, uuid.getMostSignificantBits())
                    + PbUtil.accurateFix64Size(3, uuid.getLeastSignificantBits());
        }

        @Override
        protected void encodeBody(ByteBuffer buf, ByteBufferPool pool) {
            super.writeBodySize(buf, estimateBodySize());
            PbUtil.writeFix32(buf, 1, selfNodeId);
            PbUtil.writeFix64(buf, 2, uuid.getMostSignificantBits());
            PbUtil.writeFix64(buf, 3, uuid.getLeastSignificantBits());
        }
    }

    private static class NodePingCallback extends PbCallback {
        private int nodeId;
        private long uuidHigh;
        private long uuidLow;

        public NodePingCallback() {
        }

        @Override
        public boolean readFix32(int index, int value) {
            if (index == 1) {
                this.nodeId = value;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            if (index == 2) {
                this.uuidHigh = value;
            } else if (index == 3) {
                this.uuidLow = value;
            }
            return true;
        }

        @Override
        public Object getResult() {
            return this;
        }
    }

}
