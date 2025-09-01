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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.function.Function;

/**
 * @author huangli
 */
public class NioClient extends NioNet implements ChannelListener {

    private static final DtLog log = DtLogs.getLogger(NioClient.class);

    private final NioClientConfig config;
    final NioWorker worker;

    //TODO use set?
    private final CopyOnWriteArrayList<Peer> peers;

    private final Condition connectCond = lock.newCondition();
    private int connectCount;

    long uuid1;
    long uuid2;

    public NioClient(NioClientConfig config) {
        super(config);
        this.config = config;
        UUID uuid = UUID.randomUUID();
        this.uuid1 = uuid.getMostSignificantBits();
        this.uuid2 = uuid.getLeastSignificantBits();
        ArrayList<Peer> list = new ArrayList<>();
        if (config.hostPorts != null) {
            for (HostPort hp : config.hostPorts) {
                Peer p = new Peer(hp, this);
                list.add(p);
            }
        }
        this.peers = new CopyOnWriteArrayList<>(list);
        this.worker = new NioWorker(nioStatus, config.name + "IoWorker", config, this);
        config.channelListeners.add(this);
        register(Commands.CMD_PING, new NioServer.PingProcessor());
    }

    @Override
    protected void doStart() {
        createBizExecutor();
        worker.start();
        DtTime timeout = new DtTime(config.connectTimeoutMillis, TimeUnit.MILLISECONDS);
        for (Peer peer : peers) {
            worker.connect(peer, timeout);
        }
        log.info("{} started", config.name);
    }

    @Override
    public void onConnected(DtChannel dtc) {
        lock.lock();
        try {
            connectCount++;
            connectCond.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onDisconnected(DtChannel dtc) {
        lock.lock();
        try {
            connectCount--;
            connectCond.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void waitStart(DtTime timeout) {
        if (config.hostPorts != null && !config.hostPorts.isEmpty()) {
            waitConnect(1, timeout);
        }
    }

    public void waitConnect(int targetConnectCount, DtTime timeout) {
        lock.lock();
        try {
            while (connectCount < targetConnectCount) {
                if (!connectCond.await(timeout.rest(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)) {
                    throw new NetTimeoutException("NioClient wait start timeout. timeout="
                            + timeout.getTimeout(TimeUnit.MILLISECONDS) + "ms, connectCount=" + connectCount);
                }
            }
        } catch (InterruptedException e) {
            DtUtil.restoreInterruptStatus();
            throw new NetException("Interrupted while NioClient waiting for connect", e);
        } finally {
            lock.unlock();
        }
    }

    public <T> ReadPacket<T> sendRequest(WritePacket request, DecoderCallbackCreator<T> decoder, DtTime timeout) {
        Objects.requireNonNull(decoder);
        CompletableFuture<ReadPacket<T>> f = new CompletableFuture<>();
        sendRequest(null, request, decoder, timeout, RpcCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public <T> ReadPacket<T> sendRequest(Peer peer, WritePacket request, DecoderCallbackCreator<T> decoder, DtTime timeout) {
        Objects.requireNonNull(decoder);
        CompletableFuture<ReadPacket<T>> f = new CompletableFuture<>();
        send(worker, peer, request, decoder, timeout, RpcCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public <T> void sendRequest(WritePacket request, DecoderCallbackCreator<T> decoder,
                                DtTime timeout, RpcCallback<T> callback) {
        Objects.requireNonNull(decoder);
        send(worker, null, request, decoder, timeout, callback);
    }

    public <T> void sendRequest(Peer peer, WritePacket request, DecoderCallbackCreator<T> decoder,
                                DtTime timeout, RpcCallback<T> callback) {
        Objects.requireNonNull(decoder);
        send(worker, peer, request, decoder, timeout, callback);
    }

    public CompletableFuture<Void> sendOneWay(WritePacket request, DtTime timeout) {
        return sendOneWay(null, request, timeout);
    }

    public CompletableFuture<Void> sendOneWay(Peer peer, WritePacket request, DtTime timeout) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        send(worker, peer, request, null, timeout, RpcCallback.fromUnwrapFuture(f));
        return f;
    }

    public <T> void sendOneWay(WritePacket request, DtTime timeout, RpcCallback<T> callback) {
        send(worker, null, request, null, timeout, callback);
    }

    public <T> void sendOneWay(Peer peer, WritePacket request, DtTime timeout, RpcCallback<T> callback) {
        send(worker, peer, request, null, timeout, callback);
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
        if (!force) {
            CompletableFuture<Void> pf = worker.prepareStop(timeout);
            try {
                long rest = timeout.rest(TimeUnit.MILLISECONDS);
                if (rest > 0) {
                    pf.get(rest, TimeUnit.MILLISECONDS);
                    log.info("client {} pre-stop done", config.name);
                } else {
                    log.warn("client {} pre-stop timeout. {}ms", config.name, timeout.getTimeout(TimeUnit.MILLISECONDS));
                    worker.logWorkerStatus();
                }
            } catch (InterruptedException e) {
                DtUtil.restoreInterruptStatus();
            } catch (TimeoutException e) {
                log.warn("client {} pre-stop timeout. {}ms", config.name, timeout.getTimeout(TimeUnit.MILLISECONDS));
                worker.logWorkerStatus();
            } catch (ExecutionException e) {
                BugLog.log(e);
            }
        }
        stopWorker(worker, timeout);
        try {
            long rest = timeout.rest(TimeUnit.MILLISECONDS);
            if (rest > 0) {
                worker.thread.join(rest);
            }
        } catch (InterruptedException e) {
            DtUtil.restoreInterruptStatus();
        }
        shutdownBizExecutor(timeout);

        log.info("client {} stopped", config.name);
    }

    public List<Peer> getPeers() {
        return Collections.unmodifiableList(peers);
    }

    public CompletableFuture<Peer> addPeer(HostPort hostPort) {
        Objects.requireNonNull(hostPort);
        Peer peer = new Peer(hostPort, this);
        CompletableFuture<Peer> f = new CompletableFuture<>();
        worker.doInIoThread(() -> {
            for (Peer p : peers) {
                if (p.endPoint.equals(hostPort)) {
                    f.complete(p);
                    return;
                }
            }
            peers.add(peer);
            f.complete(peer);
        }, f);
        return f;
    }

    public CompletableFuture<Void> removePeer(Peer peer) {
        checkOwner(peer);
        CompletableFuture<Void> f = new CompletableFuture<>();
        worker.doInIoThread(() -> {
            if (!peers.contains(peer)) {
                f.complete(null);
                return;
            }
            removePeer(peer, f);
        }, f);
        return f;
    }

    private void removePeer(Peer peer, CompletableFuture<Void> f) {
        if (peer.dtChannel != null) {
            worker.close(peer.dtChannel);
        }
        peers.remove(peer);
        peer.cleanWaitingConnectList(wd -> new NetException("peer removed"));
        peer.status = PeerStatus.removed;
        f.complete(null);
    }

    public CompletableFuture<Void> removePeer(HostPort hp) {
        Objects.requireNonNull(hp);
        CompletableFuture<Void> f = new CompletableFuture<>();
        worker.doInIoThread(() -> {
            Peer peer = null;
            for (Peer p : peers) {
                if (p.endPoint.equals(hp)) {
                    peer = p;
                    break;
                }
            }
            if (peer != null) {
                removePeer(peer, f);
            } else {
                f.complete(null);
            }
        }, f);
        return f;
    }

    // if ts is null, then clean all
    void cleanWaitConnectReq(Function<WriteData, NetException> exceptionSupplier) {
        // O(n)
        List<Peer> list = this.peers;
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, size = list.size(); i < size; i++) {
            Peer p = list.get(i);
            p.cleanWaitingConnectList(exceptionSupplier);
        }
    }

    /**
     * This method is idempotent.
     */
    public CompletableFuture<Void> connect(Peer peer) {
        checkOwner(peer);
        return worker.connect(peer, new DtTime(config.connectTimeoutMillis, TimeUnit.MILLISECONDS));
    }

    public CompletableFuture<Void> disconnect(Peer peer) {
        checkOwner(peer);
        return worker.disconnect(peer);
    }

    private void checkOwner(Peer peer) {
        if (peer.owner != this) {
            throw new IllegalArgumentException("the peer is not owned by this client");
        }
    }

    public NioClientConfig getConfig() {
        return config;
    }

    protected void processServerConfigHint(@SuppressWarnings("unused") Peer peer, ConfigBody cb) {
        int serverCount = peers.size();
        int v = computeHint(cb.maxPacketSize, serverCount);
        if (v > 0 && v < config.maxPacketSize) {
            config.maxPacketSize = v;
        }
        v = computeHint(cb.maxBodySize, serverCount);
        if (v > 0 && v < config.maxBodySize) {
            config.maxBodySize = v;
        }
        v = computeHint(cb.maxOutPending, serverCount);
        if (v > 0 && v < config.maxOutRequests) {
            config.maxOutRequests = v;
        }
        long v2 = computeHint(cb.maxOutPendingBytes, serverCount);
        if (v2 > 0 && v2 < config.maxOutBytes) {
            config.maxOutBytes = v2;
        }
    }

    private int computeHint(int serverHint, int serverCount) {
        if (serverHint <= 0) {
            return 0;
        }
        int v = serverHint * serverCount;
        return v < 0 ? Integer.MAX_VALUE : v;
    }

    private long computeHint(long serverHint, int serverCount) {
        if (serverHint <= 0) {
            return 0;
        }
        long v = serverHint * serverCount;
        return v < 0 ? Long.MAX_VALUE : v;
    }
}