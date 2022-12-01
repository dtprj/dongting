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
package com.github.dtprj.dongting.raft;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Decoder;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.Peer;
import com.github.dtprj.dongting.net.ProcessContext;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.StringFieldDecoder;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.net.ZeroCopyWriteFrame;
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.pb.PbUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author huangli
 */
class GroupConManager {
    private static final DtLog log = DtLogs.getLogger(GroupConManager.class);
    private final UUID uuid = UUID.randomUUID();
    private final int id;
    private final Set<HostPort> allServers;
    private final byte[] serversStr;
    private final NioClient client;
    private volatile HostPort self;
    private static final PbZeroCopyDecoder DECODER = new PbZeroCopyDecoder() {
        @Override
        protected PbCallback createCallback(ProcessContext context) {
            return new RaftInitFrameCallback(context.getIoThreadStrDecoder());
        }

        @Override
        protected Object getResult(PbCallback callback) {
            return callback;
        }
    };

    private final ReqProcessor processor = new ReqProcessor() {
        @Override
        public WriteFrame process(ReadFrame frame, ProcessContext context) {
            return new RaftInitWriteFrame();
        }

        @Override
        public Decoder getDecoder() {
            return DECODER;
        }
    };

    public GroupConManager(int id, Set<HostPort> allServers, String serversStr, NioClient client) {
        this.id = id;
        this.allServers = allServers;
        this.serversStr = serversStr.getBytes(StandardCharsets.UTF_8);
        this.client = client;
    }

    public CompletableFuture<List<Peer>> fetch() {
        ArrayList<Peer> peers = new ArrayList<>();
        CompletableFuture<List<Peer>> future = new CompletableFuture<>();
        AtomicInteger count = new AtomicInteger(allServers.size());
        for (HostPort hp : allServers) {
            if (hp.equals(self)) {
                decr(future, peers, count);
                continue;
            }
            client.addPeer(hp).whenComplete((peer, ex) -> {
                if (peer != null) {
                    if (peer.isConnected()) {
                        peers.add(peer);
                        decr(future, peers, count);
                    } else {
                        client.connect(peer).whenComplete((v, ex2) -> whenConnected(future, count, peers, peer, ex2));
                    }
                } else {
                    BugLog.getLog().error("add peer fail: {}", hp);
                    decr(future, peers, count);
                }
            });
        }
        return future;
    }

    private void decr(CompletableFuture<List<Peer>> future, ArrayList<Peer> peers, AtomicInteger count) {
        int x = count.decrementAndGet();
        if (x == 0) {
            future.complete(peers);
        }
    }

    private void whenConnected(CompletableFuture<List<Peer>> future, AtomicInteger count,
                               ArrayList<Peer> peers, Peer peer, Throwable ex) {
        if (ex != null) {
            log.info("connect to raft server {} fail: {}", peer.getEndPoint(), ex.getMessage());
            decr(future, peers, count);
        } else {
            DtTime timeout = new DtTime(10, TimeUnit.SECONDS);
            CompletableFuture<ReadFrame> f = client.sendRequest(new RaftInitWriteFrame(), DECODER, timeout);
            f.whenComplete((rf, rpcEx) -> whenRpcFinish(rf, future, count, peers, peer, rpcEx));
        }
    }

    private void whenRpcFinish(ReadFrame rf, CompletableFuture<List<Peer>> future, AtomicInteger count,
                               ArrayList<Peer> peers, Peer peer, Throwable ex) {
        if (ex != null) {
            log.info("init raft server {} fail: {}", peer.getEndPoint(), ex.getMessage());
            decr(future, peers, count);
        } else {
            RaftInitFrameCallback callback = (RaftInitFrameCallback) rf.getBody();
            if (callback.uuidHigh == uuid.getMostSignificantBits() && callback.uuidLow == uuid.getLeastSignificantBits()) {
                self = peer.getEndPoint();
                decr(future, peers, count);
            } else {
                if (callback.id == this.id) {
                    String errorMsg = "config error. duplicated id " + this.id + ", remote peer is " + peer;
                    log.error(errorMsg);
                    future.completeExceptionally(new RaftException(errorMsg));
                } else {
                    Set<HostPort> remoteServers = RaftServer.parseServers(callback.serversStr);
                    if (!allServers.equals(remoteServers)) {
                        String errorMsg = "config error. server list not same , remote peer is " + peer;
                        log.error(errorMsg);
                        future.completeExceptionally(new RaftException(errorMsg));
                    } else {
                        peers.add(peer);
                        decr(future, peers, count);
                    }
                }
            }
        }
    }

    public ReqProcessor getProcessor() {
        return processor;
    }

    class RaftInitWriteFrame extends ZeroCopyWriteFrame {

        @Override
        protected int accurateBodySize() {
            return PbUtil.accurateFix32Size(1, id)
                    + PbUtil.accurateFix64Size(2, uuid.getMostSignificantBits())
                    + PbUtil.accurateFix64Size(3, uuid.getLeastSignificantBits())
                    + PbUtil.accurateLengthDelimitedSize(4, serversStr.length);
        }

        @Override
        protected void encodeBody(ByteBuffer buf) {
            PbUtil.writeFix32(buf, 1, id);
            PbUtil.writeFix64(buf, 2, uuid.getMostSignificantBits());
            PbUtil.writeFix64(buf, 3, uuid.getLeastSignificantBits());
            PbUtil.writeBytes(buf, 4, serversStr);
        }
    }


    static class RaftInitFrameCallback extends PbCallback {
        private final StringFieldDecoder ioThreadStrDecoder;
        private int id;
        private long uuidHigh;
        private long uuidLow;
        private String serversStr;

        public RaftInitFrameCallback(StringFieldDecoder ioThreadStrDecoder) {
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
    }
}
