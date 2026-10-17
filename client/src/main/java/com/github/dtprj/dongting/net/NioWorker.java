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

import com.github.dtprj.dongting.buf.Buffers;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtThread;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.PerfConsts;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.common.VersionFactory;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * each worker represent a thread.
 *
 * @author huangli
 */
class NioWorker extends AbstractLifeCircle implements Runnable {
    private static final DtLog log = DtLogs.getLogger(NioWorker.class);

    static long incomingConnectTimeout = 5 * 1000 * 1000 * 1000L;

    private final String workerName;
    final WorkerThread thread;
    private final NioStatus nioStatus;
    private final NioConfig config;
    final NioNet owner;
    private Selector selector;
    private final AtomicInteger wakeupCalledInOtherThreads = new AtomicInteger(0);
    private boolean wakeupCalled;

    final boolean server;

    private final CompletableFuture<Void> prepareStopFuture = new CompletableFuture<>();

    private int channelIndex;
    final ArrayList<DtChannelImpl> channelsList;// client side only
    private final IntObjMap<DtChannelImpl> channels;
    private final IoWorkerQueue ioWorkerQueue;

    private final Timestamp timestamp = new Timestamp();

    private final LinkedList<DtChannelImpl> incomingConnects;//server side only
    private final LinkedList<ConnectInfo> outgoingConnects;//client side only

    private final Buffers buffers;

    final WorkerStatus workerStatus;

    private RefBuffer readBufferRef;
    private ByteBuffer readBuffer;
    private long readBufferUseTime;

    private final long cleanIntervalNanos;
    long maxPacketDecodeNanos = 30 * 1000L * 1000 * 1000;
    private long lastCleanNanos;

    private final PerfCallback perfCallback;

    public NioWorker(NioStatus nioStatus, String workerName, NioConfig config, NioNet owner) {
        this.nioStatus = nioStatus;
        this.config = config;
        this.owner = owner;
        this.workerName = workerName;
        this.cleanIntervalNanos = config.cleanInterval * 1000 * 1000;
        this.perfCallback = config.perfCallback;

        this.channels = new IntObjMap<>();
        this.server = owner instanceof NioServer;
        this.ioWorkerQueue = new IoWorkerQueue(this, config);
        if (server) {
            this.channelsList = null;
            this.outgoingConnects = null;
            this.incomingConnects = new LinkedList<>();
        } else {
            this.channelsList = new ArrayList<>();
            this.outgoingConnects = new LinkedList<>();
            this.incomingConnects = null;
        }

        BiFunction<RefBuffer, Consumer<RefBuffer>, Boolean> callback = (rb, c) ->
                ioWorkerQueue.scheduleFromBizThread(() -> c.accept(rb));
        this.buffers = config.poolFactory.createPool(timestamp);
        this.thread = new WorkerThread(this, workerName, timestamp, buffers);
        config.poolFactory.initPool(buffers, thread, callback);

        workerStatus = new WorkerStatus(this, ioWorkerQueue,
                buffers, timestamp, config.nearTimeoutThreshold);
    }

    @Override
    public void run() {
        Selector selector = this.selector;
        Timestamp ts = this.timestamp;
        ts.refresh();
        while (this.status <= STATUS_PREPARE_STOP) {
            run0(selector, ts);
        }
        try {
            ioWorkerQueue.close();
            while (!ioWorkerQueue.dispatchFinished()) {
                ioWorkerQueue.dispatchActions();
            }
            selector.close();

            workerStatus.cleanAllPendingReq();

            List<DtChannelImpl> tempList = new ArrayList<>(channels.size());
            // can't modify channels map in foreach
            channels.forEach((index, dtc) -> {
                tempList.add(dtc);
            });
            for (DtChannelImpl dtc : tempList) {
                if (dtc.peer != null) {
                    dtc.peer.shouldAutoReconnect = false;
                }
                close(dtc);
            }

            if (!server) {
                cleanOutgoingTimeoutConnect(ts);
                ((NioClient) owner).cleanWaitConnectReq(wd -> new NetException("worker closed"));
            }
            if (readBuffer != null) {
                releaseReadBuffer();
            }

            config.poolFactory.destroyPool(buffers);

            log.info("worker thread [{}] finished.", thread.getName());
        } catch (Throwable e) {
            log.error("close error. {}", e);
        }
    }

    private void run0(Selector selector, Timestamp ts) {
        long workStartTime = 0;
        try {
            int selResult;
            try {
                selResult = sel(selector, ts);
            } finally {
                workStartTime = perfCallback.takeTime(PerfConsts.RPC_D_WORKER_WORK, ts);
            }
            if (selResult >= 0) {
                ioWorkerQueue.dispatchActions();
            }
            if (selResult > 0) {
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    processOneSelectionKey(key, status, ts);
                    iterator.remove();
                }
            }
            if (status >= STATUS_PREPARE_STOP) {
                ioWorkerQueue.dispatchActions();
                if (workerStatus.packetsToWrite == 0 && workerStatus.pendingReqSize() == 0) {
                    prepareStopFuture.complete(null);
                }
            }
            if (ts.nanoTime - lastCleanNanos > cleanIntervalNanos || cleanIntervalNanos <= 0) {
                if (readBuffer != null && ts.nanoTime - readBufferUseTime > cleanIntervalNanos) {
                    releaseReadBuffer();
                }
                workerStatus.cleanPendingReqByTimeout();
                cleanDecodeTimeoutChannels(ts);
                if (server) {
                    cleanIncomingConnects(ts);
                } else {
                    // clean by request timeout
                    ((NioClient) owner).cleanWaitConnectReq(wd -> {
                        if (wd.timeout.isTimeout(timestamp)) {
                            return new NetTimeoutException("wait connect timeout");
                        }
                        return null;
                    });

                    // clean by timeout connect
                    cleanOutgoingTimeoutConnect(ts);

                    if (status == STATUS_RUNNING) {
                        tryReconnect(ts);
                    }
                }
                buffers.clean();
                lastCleanNanos = ts.nanoTime;
            }
        } catch (Throwable e) {
            log.error("NioWorker loop exception", e);
        } finally {
            workEnd(ts, workStartTime);
        }
    }

    private void cleanDecodeTimeoutChannels(Timestamp ts) {
        DtChannelImpl c = workerStatus.decodeListHead;
        while (c != null && ts.nanoTime - c.packetStartTimeNanos >= maxPacketDecodeNanos) {
            DtChannelImpl next = c.decodeNext;
            workerStatus.removeFromDecodeList(c);
            c.closeOnDecodeTimeout(ts);
            c = next;
        }
    }

    private void workEnd(Timestamp ts, long startTime) {
        PerfCallback c = perfCallback;
        if (c.accept(PerfConsts.RPC_D_WORKER_SEL) || c.accept(PerfConsts.RPC_D_WORKER_WORK)) {
            perfCallback.refresh(ts);
        } else {
            ts.refresh(1);
        }
        c.fireTime(PerfConsts.RPC_D_WORKER_WORK, startTime, 1, 0, ts);
    }

    private int sel(Selector selector, Timestamp ts) {
        PerfCallback c = perfCallback;
        boolean selNow = this.wakeupCalled || wakeupCalledInOtherThreads.get() > 0;
        long start = selNow ? 0 : c.takeTime(PerfConsts.RPC_D_WORKER_SEL, ts);
        wakeupCalledInOtherThreads.set(0);
        try {
            if (selNow) {
                return selector.selectNow();
            } else {
                long selectTimeoutMillis = config.selectTimeout;
                if (selectTimeoutMillis > 0) {
                    return selector.select(selectTimeoutMillis);
                } else {
                    // for unit test find more problem
                    return selector.select();
                }
            }
        } catch (Exception e) {
            log.error("select failed: {}", workerName, e);
            LockSupport.parkNanos(100 * 1000 * 1000L);
            return -1;
        } finally {
            if ((c.accept(PerfConsts.RPC_D_WORKER_WORK) || c.accept(PerfConsts.RPC_D_WORKER_SEL)) && !selNow) {
                perfCallback.refresh(ts);
            } else {
                ts.refresh(1);
            }
            if (!selNow) {
                c.fireTime(PerfConsts.RPC_D_WORKER_SEL, start, 1, 0, ts);
            }
            wakeupCalled = false;
        }
    }

    private void prepareReadBuffer(Timestamp roundTime) {
        if (readBuffer == null) {
            readBufferRef = workerStatus.buffers.borrowDirectLocal(config.readBufferSize);
            readBuffer = readBufferRef.getBuffer();
        }
        readBuffer.clear();
        readBufferUseTime = roundTime.nanoTime;
    }

    private void releaseReadBuffer() {
        readBufferRef.release();
        readBufferRef = null;
        this.readBuffer = null;
        readBufferUseTime = 0;
    }

    private void processOneSelectionKey(SelectionKey key, int status, Timestamp roundTime) {
        SocketChannel sc = (SocketChannel) key.channel();
        String stage = "check selection key valid";
        try {
            if (!key.isValid()) {
                log.info("socket may closed, remove it: {}", key.channel());
                closeChannelBySelKey(key, null);
                return;
            }
            stage = "process socket connect";
            if (key.isConnectable()) {
                whenConnected(key);
                return;
            }

            stage = "process socket read";
            DtChannelImpl dtc = (DtChannelImpl) key.attachment();
            if (key.isReadable()) {
                prepareReadBuffer(roundTime);
                long startTime = perfCallback.takeTimeAndRefresh(PerfConsts.RPC_D_READ, roundTime);
                int readBytes = sc.read(readBuffer);
                if (readBytes == -1) {
                    // log.info("socket read to end, remove it: {}", key.channel());
                    closeChannelBySelKey(key, null);
                    return;
                }
                perfCallback.fireTimeAndRefresh(PerfConsts.RPC_D_READ, startTime, 1, readBytes, roundTime);
                readBuffer.flip();
                dtc.afterRead(status == STATUS_RUNNING, readBuffer);
            }
            stage = "process socket write";
            if (!dtc.closed && key.isWritable()) {
                dtc.subQueue.processWriteEvent(sc, key, roundTime);
            }
        } catch (NetException | IOException e) {
            log.warn("{} error, channel will close: {}", stage, e.toString());
            closeChannelBySelKey(key, e);
        } catch (Throwable e) {
            log.warn("{} error, channel will close", stage, e);
            closeChannelBySelKey(key, e);
        }
    }

    private void wakeup() {
        if (Thread.currentThread() == thread) {
            wakeupCalled = true;
            return;
        }
        if (wakeupCalledInOtherThreads.get() == 0 && wakeupCalledInOtherThreads.incrementAndGet() == 1) {
            selector.wakeup();
        }
    }

    void markWakeupInIoThread() {
        wakeupCalled = true;
    }

    private DtChannelImpl initNewChannel(SocketChannel sc, Peer peer) throws IOException {
        sc.configureBlocking(false);
        sc.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
        sc.setOption(StandardSocketOptions.TCP_NODELAY, true);

        // 32 bit channelIndex may overflow, use channels map to keep it unique in every worker
        while (channels.get(channelIndex) != null) {
            channelIndex++;
        }
        // the worker thread is multiplexing, so can't use threadLocal DecodeContext
        DecodeContext decodeContext = DecodeContext.factory.apply(workerStatus.buffers, DtThread.currentDtThread().threadLocalBuffer);
        DtChannelImpl dtc = new DtChannelImpl(nioStatus, workerStatus, config, peer, sc, channelIndex++, decodeContext);
        SelectionKey selectionKey = sc.register(selector, SelectionKey.OP_READ, dtc);
        dtc.subQueue.setSelectionKey(selectionKey);

        log.info("new DtChannel init: {}", sc);
        return dtc;
    }

    static class ConnectInfo {
        CompletableFuture<Void> future;
        Peer peer;
        SocketChannel channel;
        DtTime deadline;
        boolean byAutoRetry;
    }

    // invoke by NioServer accept thread
    public void newChannelAccept(SocketChannel sc) {
        doInIoThread(() -> {
            if (status >= STATUS_STOPPING) {
                log.info("accept new channel while worker closed, ignore it: {}", sc);
                closeChannel0(sc);
                return;
            }
            DtChannelImpl dtc;
            try {
                dtc = initNewChannel(sc, null);
            } catch (Throwable e) {
                log.warn("accept channel fail: {}, {}", sc, e.toString());
                closeChannel0(sc);
                return;
            }
            channels.put(dtc.channelIndexInWorker, dtc);
            incomingConnects.addLast(dtc);
        }, null);
    }

    void finishHandshake(DtChannelImpl dtc) {
        dtc.handshake = true;
        if (incomingConnects != null) {
            //server side
            incomingConnects.remove(dtc);
        }
        if (!config.channelListeners.isEmpty()) {
            for (int size = config.channelListeners.size(), i = 0; i < size; i++) {
                try {
                    ChannelListener l = config.channelListeners.get(i);
                    l.onConnected(dtc);
                } catch (Throwable e) {
                    log.error("channelListener.onConnected error: {}", e);
                } finally {
                    dtc.listenerOnConnectedCalled = true;
                }
            }
        }
    }

    // invoke by other threads
    public CompletableFuture<Void> connect(Peer peer, DtTime deadline) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        if (status >= STATUS_PREPARE_STOP) {
            f.completeExceptionally(new NetException("worker closed"));
        } else {
            doInIoThread(() -> doConnect(f, peer, deadline, false), f);
        }
        return f;
    }

    void doConnect(CompletableFuture<Void> f, Peer peer, DtTime deadline, boolean byAutoRetry) {
        if (status >= STATUS_PREPARE_STOP) {
            f.completeExceptionally(new NetException("worker closed"));
            return;
        }
        PeerStatus s = peer.status;
        if (s == PeerStatus.not_connect) {
            peer.shouldAutoReconnect = true;
            peer.status = PeerStatus.connecting;
        } else if (s == PeerStatus.removed) {
            NetException ex = new NetException("peer is removed");
            f.completeExceptionally(ex);
            return;
        } else if (s == PeerStatus.connected) {
            f.complete(null);
            return;
        } else {
            // connecting, handshake
            peer.connectInfo.future.whenComplete((v, ex) -> {
                if (ex == null) {
                    f.complete(null);
                } else {
                    f.completeExceptionally(ex);
                }
            });
            return;
        }
        SocketChannel sc = null;
        ConnectInfo ci = null;
        try {
            HostPort hp = peer.endPoint;
            InetSocketAddress addr = new InetSocketAddress(hp.getHost(), hp.getPort());
            sc = SocketChannel.open();
            sc.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
            sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
            sc.configureBlocking(false);

            ci = new ConnectInfo();
            ci.future = f;
            ci.peer = peer;
            ci.channel = sc;
            ci.deadline = deadline;
            ci.byAutoRetry = byAutoRetry;
            outgoingConnects.add(ci);
            peer.connectInfo = ci;

            sc.register(selector, SelectionKey.OP_CONNECT, ci);
            sc.connect(addr);
        } catch (Throwable e) {
            if (ci != null) {
                failConnect(ci, new NetException(e));
            } else {
                if (sc != null) {
                    closeChannel0(sc);
                }
                peer.markNotConnect(config, workerStatus, byAutoRetry);
                peer.connectInfo = null;
                NetException netEx = new NetException(e);
                peer.cleanWaitingConnectList(wd -> netEx);
                f.completeExceptionally(netEx);
            }
        }
    }

    private void failConnect(ConnectInfo ci, NetException ex) {
        outgoingConnects.remove(ci);
        Peer peer = ci.peer;
        peer.connectInfo = null;
        closeChannel0(ci.channel);
        if (peer.status != PeerStatus.removed) {
            peer.markNotConnect(config, workerStatus, ci.byAutoRetry);
        }
        peer.cleanWaitingConnectList(wd -> ex);
        ci.future.completeExceptionally(ex);
    }

    // called client side
    private void whenConnected(SelectionKey key) {
        ConnectInfo ci = (ConnectInfo) key.attachment();
        outgoingConnects.remove(ci);
        DtChannelImpl dtc;
        SocketChannel channel = ci.channel;
        if (ci.future.isDone()) {
            return;
        }
        if (status >= STATUS_PREPARE_STOP) {
            failConnect(ci, new NetException("worker closed"));
            return;
        }
        try {
            channel.finishConnect();
            dtc = initNewChannel(channel, ci.peer);
            channels.put(dtc.channelIndexInWorker, dtc);
            channelsList.add(dtc);
        } catch (Throwable e) {
            log.warn("connect channel fail: {}, {}", ci.peer.endPoint, e.toString());
            failConnect(ci, new NetException(e));
            return;
        }

        Peer peer = ci.peer;
        peer.dtChannel = dtc;
        peer.status = PeerStatus.handshake;

        try {
            sendHandshake(dtc, ci);
        } catch (Throwable e) {
            log.error("send handshake fail: {}", e);
            ci.future.completeExceptionally(e);
            close(dtc, e);
        }
    }

    private void sendHandshake(DtChannelImpl dtc, ConnectInfo ci) {
        RpcCallback<HandshakeBody> rpcCallback = (resp, ex) -> {
            if (ex != null) {
                log.error("handshake fail: {}", ex);
                ci.future.completeExceptionally(ex);
                close(dtc, ex);
            } else {
                handshakeSuccess(dtc, ci, resp);
            }
        };

        HandshakeBody hb = new HandshakeBody();
        hb.majorVersion = DtUtil.RPC_MAJOR_VER;
        hb.minorVersion = DtUtil.RPC_MINOR_VER;
        ProcessInfoBody pb = new ProcessInfoBody();
        NioClient client = (NioClient) owner;
        pb.uuid1 = client.uuid1;
        pb.uuid2 = client.uuid2;
        hb.processInfo = pb;
        ConfigBody cb = new ConfigBody();
        cb.maxPacketSize = config.maxPacketSize;
        cb.maxBodySize = config.maxBodySize;
        cb.maxOutPending = config.maxOutRequests;
        cb.maxOutPendingBytes = config.maxOutBytes;
        cb.maxInPending = config.maxInRequests;
        cb.maxInPendingBytes = config.maxInBytes;
        hb.config = cb;
        SimpleWritePacket p = new SimpleWritePacket(hb);

        p.packetType = PacketType.TYPE_REQ;
        p.command = Commands.CMD_HANDSHAKE;

        PacketInfoReq wd = new PacketInfoReq(null, null, dtc, p, ci.deadline, rpcCallback,
                ctx -> ctx.toDecoderCallback(new HandshakeBody()));
        dtc.subQueue.enqueue(wd);
        // send pending request as quickly as possible, even before handshake finished
        ci.peer.enqueueAfterConnect();
    }

    private void handshakeSuccess(DtChannelImpl dtc, ConnectInfo ci, ReadPacket<HandshakeBody> resp) {
        if (status >= STATUS_PREPARE_STOP) {
            log.info("handshake while worker closed, ignore it: {}", dtc.getChannel());
            NetException e = new NetException("worker closed");
            ci.future.completeExceptionally(e);
            close(dtc, e);
            return;
        }
        if (resp.getBody() == null || resp.getBody().config == null) {
            log.error("handshake fail: invalid response");
            NetException e = new NetException("invalid handshake response");
            ci.future.completeExceptionally(e);
            close(dtc, e);
            return;
        }
        ConfigBody cb = resp.getBody().config;
        if (config != null && config.serverHint) {
            ((NioClient) owner).processServerConfigHint(dtc.peer, cb);
            VersionFactory.getInstance().fullFence();
        }

        ci.peer.status = PeerStatus.connected;
        ci.peer.resetConnectRetry(workerStatus);
        finishHandshake(dtc);
        ci.peer.enqueueAfterConnect();
        ci.future.complete(null);
        ci.peer.connectInfo = null;
    }

    public void doInIoThread(Runnable runnable, CompletableFuture<?> future) {
        if (Thread.currentThread() == thread) {
            runnable.run();
        } else {
            boolean b = ioWorkerQueue.scheduleFromBizThread(runnable);
            wakeup();
            if (!b && future != null) {
                future.completeExceptionally(new NetException("closed"));
            }
        }
    }

    public CompletableFuture<Void> disconnect(Peer peer) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        boolean b = ioWorkerQueue.scheduleFromBizThread(() -> {
            peer.shouldAutoReconnect = false;
            ConnectInfo ci = peer.connectInfo;
            if (ci != null && peer.dtChannel == null) {
                // cancel the in-flight connect, otherwise whenConnected creates a channel out of peers list
                failConnect(ci, new NetException("peer disconnected"));
            }
            if (peer.dtChannel == null) {
                f.complete(null);
                return;
            }
            if (ci != null) {
                // handshake in progress: fail requests that arrived after sendHandshake drained the list
                peer.cleanWaitingConnectList(wd -> new NetException("peer disconnected"));
            }
            try {
                close(peer.dtChannel);
                f.complete(null);
            } catch (Throwable e) {
                f.completeExceptionally(e);
            }
        });
        wakeup();
        if (!b) {
            // worker is stopping, run() closes all channels and connects
            log.info("worker closed, ignore disconnect: {}", peer.endPoint);
            f.complete(null);
        }
        return f;
    }

    private void closeChannelBySelKey(SelectionKey key, Throwable ex) {
        Object obj = key.attachment();
        SocketChannel sc = (SocketChannel) key.channel();
        if (obj instanceof DtChannelImpl) {
            DtChannelImpl dtc = (DtChannelImpl) obj;
            close(dtc, ex);
        } else if (obj instanceof ConnectInfo) {
            // the connect was cancelled (e.g. by disconnect) but the key is still in the selected set
            closeChannel0(sc);
        } else {
            BugLog.log("assert false. key attachment is not " + DtChannelImpl.class.getName());
            closeChannel0(sc);
            assert false;
        }
    }

    void close(DtChannelImpl dtc) {
        close(dtc, null);
    }

    void close(DtChannelImpl dtc, Throwable ex) {
        if (dtc.closed) {
            return;
        }
        if (dtc.encodingOrDecoding) {
            // the channel's read/write callback is on the stack, executePendingClose closes it when the callback returns
            dtc.closePending = true;
            if (ex != null && dtc.closeCause == null) {
                dtc.closeCause = ex;
            }
            return;
        }
        doClose(dtc, ex);
    }

    void executePendingClose(DtChannelImpl dtc) {
        if (dtc.closePending && !dtc.closed) {
            doClose(dtc, dtc.closeCause);
        }
    }

    private void doClose(DtChannelImpl dtc, Throwable ex) {
        dtc.closed = true;
        dtc.close(ex);

        if (server) {
            incomingConnects.remove(dtc);
            if (dtc.remoteUuid != null) {
                // the uuid may be taken over by a new re-connected channel, only remove self
                ((NioServer) owner).getClients().remove(dtc.remoteUuid, dtc);
            }
        } else {
            Peer p = dtc.peer;
            p.dtChannel = null;
            if (p.status != PeerStatus.removed) {
                p.markNotConnect(config, workerStatus, false);
            }
            if (p.connectInfo != null) {
                if (!p.connectInfo.future.isDone()) {
                    p.connectInfo.future.completeExceptionally(new NetException("channel closed"));
                }
                p.connectInfo = null;
            }
            // O(n) in client side
            channelsList.remove(dtc);
        }
        channels.remove(dtc.channelIndexInWorker);
        closeChannel0(dtc.getChannel());
        workerStatus.cleanPendingReqByChannel(dtc, ex);
        dtc.subQueue.cleanChannelQueue(ex);

        if (dtc.listenerOnConnectedCalled && !config.channelListeners.isEmpty()) {
            for (int size = config.channelListeners.size(), i = 0; i < size; i++) {
                try {
                    ChannelListener l = config.channelListeners.get(i);
                    l.onDisconnected(dtc);
                } catch (Throwable e) {
                    log.error("channelListener.onDisconnected error: {}", e);
                }
            }
        }
    }

    private void closeChannel0(SocketChannel sc) {
        try {
            if (sc.isOpen()) {
                log.info("closing channel: {}", sc);
                sc.close();
            }
        } catch (Exception e) {
            log.warn("close channel fail: {}, {}", sc, e.getMessage());
        }
    }

    private void cleanOutgoingTimeoutConnect(Timestamp roundStartTime) {
        boolean close = status >= STATUS_STOPPING;
        if (outgoingConnects.isEmpty()) {
            return;
        }
        // iterate a copy: failConnect callbacks may re-enter doConnect and structurally
        // modify the list, a live Iterator would throw ConcurrentModificationException
        List<ConnectInfo> snapshot = new ArrayList<>(outgoingConnects);
        for (ConnectInfo ci : snapshot) {
            if (close || ci.deadline.isTimeout(roundStartTime)) {
                NetException netEx;
                if (close) {
                    log.info("worker closed, connect fail: {}", ci.peer.endPoint);
                    netEx = new NetException("worker closed");
                } else {
                    log.warn("connect timeout: {}ms, {}", ci.deadline.getTimeout(TimeUnit.MILLISECONDS),
                            ci.peer.endPoint);
                    netEx = new NetTimeoutException("connect timeout");
                }
                failConnect(ci, netEx);
            }
        }
    }

    private void cleanIncomingConnects(Timestamp roundStartTime) {
        long t = roundStartTime.nanoTime - incomingConnectTimeout;
        for (Iterator<DtChannelImpl> it = this.incomingConnects.iterator(); it.hasNext(); ) {
            DtChannelImpl dtc = it.next();
            if (dtc.createTimeNanos - t < 0) {
                log.warn("incoming connect timeout: {}", dtc.getChannel());
                it.remove();
                close(dtc);
            } else {
                break;
            }
        }
    }

    private void tryReconnect(Timestamp ts) {
        if (workerStatus.retryConnect <= 0) {
            return;
        }
        List<Peer> peers = ((NioClient) owner).getPeers();
        for (Peer p : peers) {
            if (!p.shouldAutoReconnect || p.status != PeerStatus.not_connect) {
                continue;
            }
            if (ts.nanoTime - p.lastRetryNanos > 0) {
                CompletableFuture<Void> f = new CompletableFuture<>();
                DtTime deadline = new DtTime(((NioClient) owner).getConfig().connectTimeoutMillis, TimeUnit.MILLISECONDS);
                doConnect(f, p, deadline, true);
            }
        }
    }

    // invoke by other threads
    void writeReqInBizThreads(PacketInfo data) {
        this.ioWorkerQueue.writeFromBizThread(data);
        wakeup();
    }

    @Override
    public void doStart() {
        try {
            selector = SelectorProvider.provider().openSelector();
            thread.start();
        } catch (Exception e) {
            throw new NetException(e);
        }
    }

    @Override
    public CompletableFuture<Void> doPrepareStop(DtTime timeout) {
        selector.wakeup();
        return prepareStopFuture;
    }

    @Override
    public void doStop(DtTime timeout, boolean force) {
        selector.wakeup();
    }

    protected void logWorkerStatus() {
        log.info("worker={}, outgoingConnects={}, pendingOutgoingRequests={}",
                workerName, outgoingConnects == null ? 0 : outgoingConnects.size(), workerStatus.pendingReqSize());
    }
}
