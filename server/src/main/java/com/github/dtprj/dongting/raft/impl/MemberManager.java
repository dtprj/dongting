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
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.Decoder;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.Peer;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.StringFieldDecoder;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.pb.PbUtil;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author huangli
 */
public class MemberManager {
    private static final DtLog log = DtLogs.getLogger(MemberManager.class);
    private final UUID uuid = UUID.randomUUID();
    private final byte[] serversStr;
    private final RaftServerConfig config;
    private final NioClient client;
    private final Executor executor;
    private final RaftStatus raftStatus;

    private static final PbZeroCopyDecoder DECODER = new PbZeroCopyDecoder(context ->
            new RaftPingFrameCallback(context.getIoThreadStrDecoder()));

    private final ReqProcessor processor = new ReqProcessor() {
        @Override
        public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
            return new RaftPingWriteFrame();
        }

        @Override
        public Decoder getDecoder() {
            return DECODER;
        }
    };

    public MemberManager(RaftServerConfig config, NioClient client, Executor executor, RaftStatus raftStatus) {
        this.serversStr = config.getServers().getBytes(StandardCharsets.UTF_8);
        this.config = config;
        this.client = client;
        this.executor = executor;
        this.raftStatus = raftStatus;
    }

    private RaftMember find(List<RaftMember> servers, HostPort hostPort) {
        for (RaftMember node : servers) {
            if (node.getPeer().getEndPoint().equals(hostPort)) {
                return node;
            }
        }
        return null;
    }

    private CompletableFuture<List<RaftMember>> add(Collection<HostPort> endPoints) {
        HashSet<HostPort> validEndPoints = new HashSet<>();
        List<RaftMember> servers = raftStatus.getServers();
        for (HostPort hp : endPoints) {
            if (find(servers, hp) == null) {
                validEndPoints.add(hp);
            }
        }
        if (validEndPoints.size() == 0) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }

        ArrayList<CompletableFuture<Peer>> list = new ArrayList<>();
        for (HostPort hp : validEndPoints) {
            list.add(client.addPeer(hp));
        }
        return CompletableFuture.allOf(list.toArray(new CompletableFuture[0]))
                .thenApply(v -> list.stream().map(f -> new RaftMember(f.join()))
                        .collect(Collectors.toList()));
    }

    public void addSync(Collection<HostPort> endPoints) {
        CompletableFuture<List<RaftMember>> result = add(endPoints);
        List<RaftMember> list = result.join();
        List<RaftMember> servers = raftStatus.getServers();
        for (RaftMember node : list) {
            if (find(servers, node.getPeer().getEndPoint()) == null) {
                servers.add(node);
            }
        }
    }

    public void pingAllAndUpdateServers() {
        for (RaftMember node : raftStatus.getServers()) {
            if (node.isSelf()) {
                continue;
            }
            CompletableFuture<PingResult> f = connectAndPing(node);
            f.handleAsync((pingResult, ex) -> {
                if (ex == null) {
                    processPingInRaftThread(node, pingResult);
                } else {
                    // the exception should be handled
                    BugLog.log(ex);
                }
                return null;
            }, executor);
        }
    }

    private void processPingInRaftThread(RaftMember node, PingResult pingResult) {
        node.setPinging(false);
        if (pingResult == null) {
            return;
        }
        ArrayList<RaftMember> list = new ArrayList<>(raftStatus.getServers());
        list.remove(node);
        RaftMember result = new RaftMember(null);
        result.setId(pingResult.id);
        result.setReady(pingResult.ready);
        result.setServers(pingResult.servers);
        list.add(result);

        boolean checkOk = false;
        try {
            checkNodes(list);
            checkOk = true;
        } catch (RaftException e) {
            log.error(e.getMessage());
            node.setReady(false);
        }
        if (checkOk) {
            pingResult.copyTo(node);
        }
    }

    public CompletableFuture<PingResult> connectAndPing(RaftMember node) {
        if (node.isPinging()) {
            // do nothing, node status will not change
            return CompletableFuture.completedFuture(null);
        }

        node.setPinging(true);
        PingResult pingResult = new PingResult(node);
        CompletableFuture<Void> connectFuture;
        PeerStatus peerStatus = node.getPeer().getStatus();
        if (peerStatus == PeerStatus.connected) {
            connectFuture = CompletableFuture.completedFuture(null);
        } else if (peerStatus == PeerStatus.not_connect) {
            DtTime deadline = new DtTime(config.getConnectTimeout(), TimeUnit.MILLISECONDS);
            connectFuture = client.connect(node.getPeer(), deadline);
        } else {
            BugLog.getLog().error("assert false, peer status is connecting");
            return CompletableFuture.completedFuture(null);
        }
        return connectFuture.thenCompose(v -> sendRaftPing(node, pingResult))
                .exceptionally(ex -> {
                    log.info("connect to raft server {} fail: {}",
                            node.getPeer().getEndPoint(), ex.toString());
                    pingResult.ready = false;
                    return pingResult;
                });
    }

    private CompletableFuture<PingResult> sendRaftPing(RaftMember node, PingResult pingResult) {
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame> f = client.sendRequest(node.getPeer(), new RaftPingWriteFrame(), DECODER, timeout);
        return f.handle((rf, ex) -> {
            if (ex == null) {
                whenRpcFinish(rf, node, pingResult);
            } else {
                pingResult.ready = false;
                log.info("init raft connection {} fail: {}", node.getPeer().getEndPoint(), ex.getMessage());
            }
            return pingResult;
        });
    }

    // run in io thread
    private void whenRpcFinish(ReadFrame rf, RaftMember node, PingResult pingResult) {
        RaftPingFrameCallback callback = (RaftPingFrameCallback) rf.getBody();
        HashSet<HostPort> nodesSet = new HashSet<>();
        try {
            IntObjMap<HostPort> remoteServers = RaftUtil.parseServers(callback.serversStr);
            remoteServers.forEach((id, hp) -> {
                nodesSet.add(hp);
                return true;
            });
        } catch (Exception e) {
            pingResult.ready = false;
            log.error("servers list is empty", e);
            return;
        }
        boolean self = callback.uuidHigh == uuid.getMostSignificantBits()
                && callback.uuidLow == uuid.getLeastSignificantBits();
        pingResult.servers = Collections.unmodifiableSet(nodesSet);
        pingResult.id = callback.id;
        pingResult.self = self;

        pingResult.ready = true;

        if (!self) {
            log.info("init raft connection success: remote={}, remoteId={}, servers={}",
                    node.getPeer().getEndPoint(), callback.id, callback.serversStr);
        } else {
            client.disconnect(node.getPeer());
        }
    }

    private void checkSelf(List<RaftMember> list) {
        for (RaftMember rn1 : list) {
            if (rn1.isSelf()) {
                return;
            }
        }
        throw new RaftException("can't init raft connection to self");
    }

    private static void checkNodes(List<RaftMember> list) {
        int size = list.size();
        for (int i = 0; i < size; i++) {
            RaftMember rn1 = list.get(i);
            if (!rn1.isReady()) {
                continue;
            }
            for (int j = i + 1; j < size; j++) {
                RaftMember rn2 = list.get(j);
                if (!rn2.isReady()) {
                    continue;
                }
                if (rn1.getId() == rn2.getId()) {
                    throw new RaftException("find same id: " + rn1.getId() + ", "
                            + rn1.getPeer().getEndPoint() + ", " + rn2.getPeer().getEndPoint());
                }
                if (!rn1.getServers().equals(rn2.getServers())) {
                    throw new RaftException("server list not same: "
                            + rn1.getPeer().getEndPoint() + ", " + rn2.getPeer().getEndPoint());
                }
            }
        }
    }

    @SuppressWarnings("BusyWait")
    public void initRaftGroup(int electQuorum, Collection<HostPort> servers, int sleepMillis) {
        addSync(servers);
        while (true) {
            List<RaftMember> serverList = raftStatus.getServers();
            List<CompletableFuture<PingResult>> futures = new ArrayList<>();
            for (RaftMember node : serverList) {
                futures.add(connectAndPing(node));
            }
            DtTime timeout = new DtTime(config.getElectTimeout(), TimeUnit.MILLISECONDS);
            for (int i = 0; i < futures.size(); i++) {
                RaftMember node = serverList.get(i);
                CompletableFuture<PingResult> f = futures.get(i);
                long timeoutMillis = timeout.rest(TimeUnit.MILLISECONDS);
                PingResult pingResult;
                if (timeoutMillis > 0) {
                    try {
                        pingResult = f.get(timeoutMillis, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new RaftException(e);
                    } catch (TimeoutException e) {
                        continue;
                    } catch (ExecutionException e) {
                        // the exception should be handled
                        BugLog.log(e);
                        throw new RaftException(e);
                    }
                } else {
                    pingResult = f.getNow(null);
                }
                node.setPinging(false);
                if (pingResult != null) {
                    pingResult.copyTo(node);
                }
            }
            checkSelf(serverList);
            checkNodes(serverList);
            long currentNodes = serverList.stream().filter(RaftMember::isReady).count();
            if (currentNodes >= electQuorum) {
                log.info("raft group init success. electQuorum={}, currentNodes={}. remote peers: {}",
                        electQuorum, currentNodes, serverList);
                return;
            }
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                throw new RaftException(e);
            }
        }
    }

    public ReqProcessor getProcessor() {
        return processor;
    }

    class RaftPingWriteFrame extends WriteFrame {

        public RaftPingWriteFrame() {
            setCommand(Commands.RAFT_PING);
        }

        @Override
        protected int calcEstimateBodySize() {
            return PbUtil.accurateFix32Size(1, config.getId())
                    + PbUtil.accurateFix64Size(2, uuid.getMostSignificantBits())
                    + PbUtil.accurateFix64Size(3, uuid.getLeastSignificantBits())
                    + PbUtil.accurateLengthDelimitedSize(4, serversStr.length, false);
        }

        @Override
        protected void encodeBody(ByteBuffer buf, ByteBufferPool pool) {
            super.writeBodySize(buf, estimateBodySize());
            PbUtil.writeFix32(buf, 1, config.getId());
            PbUtil.writeFix64(buf, 2, uuid.getMostSignificantBits());
            PbUtil.writeFix64(buf, 3, uuid.getLeastSignificantBits());
            PbUtil.writeBytes(buf, 4, serversStr, false);
        }
    }


    static class RaftPingFrameCallback extends PbCallback {
        private final StringFieldDecoder ioThreadStrDecoder;
        private int id;
        private long uuidHigh;
        private long uuidLow;
        private String serversStr;

        public RaftPingFrameCallback(StringFieldDecoder ioThreadStrDecoder) {
            this.ioThreadStrDecoder = ioThreadStrDecoder;
        }

        @Override
        public boolean readFix32(int index, int value) {
            if (index == 1) {
                this.id = value;
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
        public boolean readBytes(int index, ByteBuffer buf, int len, boolean begin, boolean end) {
            if (index == 4) {
                serversStr = ioThreadStrDecoder.decodeUTF8(buf, len, begin, end);
            }
            return true;
        }

        @Override
        public Object getResult() {
            return this;
        }
    }

    static class PingResult {
        int id;
        Set<HostPort> servers;
        boolean self;

        boolean ready;

        PingResult(RaftMember initStatus) {
            this.id = initStatus.getId();
            this.servers = initStatus.getServers();
            this.self = initStatus.isSelf();

            this.ready = initStatus.isReady();
        }

        public void copyTo(RaftMember node) {
            node.setId(id);
            node.setServers(servers);
            node.setSelf(self);

            node.setReady(ready);

            if (!ready) {
                node.setMultiAppend(false);
            }
        }
    }

}