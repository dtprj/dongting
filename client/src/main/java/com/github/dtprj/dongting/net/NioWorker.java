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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.buf.TwoLevelPool;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
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

    private final ByteBufferPool directPool;
    private final ByteBufferPool heapPool;

    final WorkerStatus workerStatus;

    private ByteBuffer readBuffer;
    private long readBufferUseTime;

    private final long cleanIntervalNanos;
    private long lastCleanNanos;

    private final PerfCallback perfCallback;

    public NioWorker(NioStatus nioStatus, String workerName, NioConfig config, NioNet owner) {
        this.nioStatus = nioStatus;
        this.config = config;
        this.owner = owner;
        this.thread = new WorkerThread(this, workerName, timestamp);
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

        this.directPool = config.poolFactory.createPool(timestamp, true);
        this.heapPool = config.poolFactory.createPool(timestamp, false);

        ByteBufferPool releaseSafePool = createReleaseSafePool((TwoLevelPool) heapPool, ioWorkerQueue);
        RefBufferFactory refBufferFactory = new RefBufferFactory(releaseSafePool, 512);

        workerStatus = new WorkerStatus(this, ioWorkerQueue, directPool,
                refBufferFactory, timestamp, config.nearTimeoutThreshold);
    }

    private ByteBufferPool createReleaseSafePool(TwoLevelPool heapPool, IoWorkerQueue ioWorkerQueue) {
        Consumer<ByteBuffer> callback = (buf) -> {
            boolean b = ioWorkerQueue.scheduleFromBizThread(() -> heapPool.mixedRelease(buf));
            if (!b) {
                log.warn("schedule ReleaseBufferTask fail");
            }
        };
        return heapPool.toReleaseInOtherThreadInstance(thread, callback);
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
            }
            if (readBuffer != null) {
                releaseReadBuffer();
            }

            config.poolFactory.destroyPool(directPool);
            config.poolFactory.destroyPool(heapPool);

            log.info("worker thread [{}] finished.", thread.getName());
            if (DtUtil.DEBUG >= 2) {
                log.info("direct pool stat: {}\nheap pool stat: {}", directPool.formatStat(), heapPool.formatStat());
            }
        } catch (Throwable e) {
            log.error("close error. {}", e);
        }
    }

    private void run0(Selector selector, Timestamp ts) {
        long workStartTime = 0;
        try {
            boolean selOk;
            try {
                selOk = sel(selector, ts);
            } finally {
                workStartTime = perfCallback.takeTime(PerfConsts.RPC_D_WORKER_WORK, ts);
            }
            if (selOk) {
                ioWorkerQueue.dispatchActions();
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
                directPool.clean();
                heapPool.clean();
                lastCleanNanos = ts.nanoTime;
            }
        } catch (Throwable e) {
            log.error("NioWorker loop exception", e);
        } finally {
            workEnd(ts, workStartTime);
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

    private boolean sel(Selector selector, Timestamp ts) {
        PerfCallback c = perfCallback;
        boolean selNow = this.wakeupCalled || wakeupCalledInOtherThreads.get() > 0;
        long start = selNow ? 0 : c.takeTime(PerfConsts.RPC_D_WORKER_SEL, ts);
        try {
            if (selNow) {
                selector.selectNow();
            } else {
                long selectTimeoutMillis = config.selectTimeout;
                if (selectTimeoutMillis > 0) {
                    selector.select(selectTimeoutMillis);
                } else {
                    // for unit test find more problem
                    selector.select();
                }
            }
            return true;
        } catch (Exception e) {
            log.error("select failed: {}", workerName, e);
            return false;
        } finally {
            if ((c.accept(PerfConsts.RPC_D_WORKER_WORK) || c.accept(PerfConsts.RPC_D_WORKER_SEL)) && !selNow) {
                perfCallback.refresh(ts);
            } else {
                ts.refresh(1);
            }
            if (!selNow) {
                c.fireTime(PerfConsts.RPC_D_WORKER_SEL, start, 1, 0, ts);
            }
            wakeupCalledInOtherThreads.lazySet(0);
            wakeupCalled = false;
        }
    }

    private void prepareReadBuffer(Timestamp roundTime) {
        if (readBuffer == null) {
            readBuffer = directPool.borrow(config.readBufferSize);
        }
        readBuffer.clear();
        readBufferUseTime = roundTime.nanoTime;
    }

    private void releaseReadBuffer() {
        directPool.release(readBuffer);
        this.readBuffer = null;
        readBufferUseTime = 0;
    }

    private void processOneSelectionKey(SelectionKey key, int status, Timestamp roundTime) {
        SocketChannel sc = (SocketChannel) key.channel();
        String stage = "check selection key valid";
        try {
            if (!key.isValid()) {
                log.info("socket may closed, remove it: {}", key.channel());
                closeChannelBySelKey(key);
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
                    closeChannelBySelKey(key);
                    return;
                }
                perfCallback.fireTimeAndRefresh(PerfConsts.RPC_D_READ, startTime, 1, readBytes, roundTime);
                readBuffer.flip();
                dtc.afterRead(status == STATUS_RUNNING, readBuffer);
            }
            stage = "process socket write";
            if (key.isWritable()) {
                IoChannelQueue subQueue = dtc.subQueue;
                ByteBuffer buf = subQueue.getWriteBuffer(roundTime);
                if (buf != null) {
                    subQueue.setWriting(true);
                    long startTime = perfCallback.takeTimeAndRefresh(PerfConsts.RPC_D_WRITE, roundTime);
                    int x1 = buf.remaining();
                    sc.write(buf);
                    int x2 = buf.remaining();
                    if (x2 == 0) {
                        subQueue.afterBufferWriteFinish();
                    }
                    perfCallback.fireTimeAndRefresh(PerfConsts.RPC_D_WRITE, startTime, 1, x1 - x2, roundTime);
                } else {
                    // no data to write
                    subQueue.setWriting(false);
                    key.interestOps(SelectionKey.OP_READ);
                    perfCallback.fire(PerfConsts.RPC_C_MARK_READ);
                }
            }
        } catch (NetException | IOException e) {
            log.warn("{} error, channel will close: {}", stage, e.toString());
            closeChannelBySelKey(key);
        } catch (Exception e) {
            log.warn("{} error, channel will close", stage, e);
            closeChannelBySelKey(key);
        }
    }

    void wakeup() {
        if (Thread.currentThread() == thread) {
            wakeupCalled = true;
            return;
        }
        if (wakeupCalledInOtherThreads.incrementAndGet() == 1) {
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
        DtChannelImpl dtc = new DtChannelImpl(nioStatus, workerStatus, config, peer, sc, channelIndex++);
        SelectionKey selectionKey = sc.register(selector, SelectionKey.OP_READ, dtc);
        dtc.subQueue.setRegisterForWrite(new RegWriteRunner(selectionKey));

        log.info("new DtChannel init: {}", sc);
        return dtc;
    }

    private class RegWriteRunner implements Runnable {
        SelectionKey key;

        RegWriteRunner(SelectionKey key) {
            this.key = key;
        }

        @Override
        public void run() {
            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            perfCallback.fire(PerfConsts.RPC_C_MARK_WRITE);
        }
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
        try {
            HostPort hp = peer.endPoint;
            InetSocketAddress addr = new InetSocketAddress(hp.getHost(), hp.getPort());
            sc = SocketChannel.open();
            sc.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
            sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
            sc.configureBlocking(false);

            ConnectInfo ci = new ConnectInfo();
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
            // do clean in cleanTimeoutConnect() called in run()
            return;
        }
        try {
            channel.finishConnect();
            dtc = initNewChannel(channel, ci.peer);
            channels.put(dtc.channelIndexInWorker, dtc);
            channelsList.add(dtc);
        } catch (Throwable e) {
            log.warn("connect channel fail: {}, {}", ci.peer.endPoint, e.toString());
            closeChannel0(channel);
            ci.peer.markNotConnect(config, workerStatus, ci.byAutoRetry);
            ci.peer.connectInfo = null;
            NetException netEx = new NetException(e);
            ci.peer.cleanWaitingConnectList(wd -> netEx);
            ci.future.completeExceptionally(netEx);
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
            close(dtc);
        }
    }

    private void sendHandshake(DtChannelImpl dtc, ConnectInfo ci) {
        RpcCallback<HandshakeBody> rpcCallback = (resp, ex) -> {
            if (ex != null) {
                log.error("handshake fail: {}", ex);
                ci.future.completeExceptionally(ex);
                closeLater(dtc);
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
            ci.future.completeExceptionally(new NetException("worker closed"));
            closeLater(dtc);
            return;
        }
        if (resp.getBody() == null || resp.getBody().config == null) {
            log.error("handshake fail: invalid response");
            ci.future.completeExceptionally(new NetException("invalid handshake response"));
            closeLater(dtc);
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
            if (!b) {
                if (future != null) {
                    future.completeExceptionally(new NetException("closed"));
                }
            }
        }
    }

    public CompletableFuture<Void> disconnect(Peer peer) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        doInIoThread(() -> {
            peer.shouldAutoReconnect = false;
            if (peer.dtChannel == null) {
                f.complete(null);
                return;
            }
            try {
                close(peer.dtChannel);
                f.complete(null);
            } catch (Throwable e) {
                f.completeExceptionally(e);
            }
        }, f);
        return f;
    }

    private void closeChannelBySelKey(SelectionKey key) {
        Object obj = key.attachment();
        SocketChannel sc = (SocketChannel) key.channel();
        if (obj instanceof DtChannelImpl) {
            DtChannelImpl dtc = (DtChannelImpl) obj;
            close(dtc);
        } else {
            BugLog.getLog().error("assert false. key attachment is not " + DtChannelImpl.class.getName());
            closeChannel0(sc);
            assert false;
        }
    }

    // make sure not re-use tempSortList when iterate it (callFail may cause use tempSortList again during close dtc)
    private void closeLater(DtChannelImpl dtc) {
        ioWorkerQueue.scheduleFromBizThread(() -> close(dtc));
    }

    void close(DtChannelImpl dtc) {
        if (dtc.isClosed()) {
            return;
        }

        dtc.close();

        if (server) {
            incomingConnects.remove(dtc);
            if (dtc.remoteUuid != null) {
                ((NioServer) owner).getClients().remove(dtc.remoteUuid);
            }
        } else {
            Peer p = dtc.peer;
            p.dtChannel = null;
            p.markNotConnect(config, workerStatus, false);
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
        workerStatus.cleanPendingReqByChannel(dtc);
        dtc.subQueue.cleanChannelQueue();

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
        for (Iterator<ConnectInfo> it = this.outgoingConnects.iterator(); it.hasNext(); ) {
            ConnectInfo ci = it.next();
            if (close || ci.deadline.isTimeout(roundStartTime)) {
                ci.peer.markNotConnect(config, workerStatus, ci.byAutoRetry);
                ci.peer.connectInfo = null;
                NetException netEx;
                if (close) {
                    log.info("worker closed, connect fail: {}", ci.peer.endPoint);
                    netEx = new NetException("worker closed");
                } else {
                    log.warn("connect timeout: {}ms, {}", ci.deadline.getTimeout(TimeUnit.MILLISECONDS),
                            ci.peer.endPoint);
                    netEx = new NetTimeoutException("connect timeout");
                }
                closeChannel0(ci.channel);
                ci.peer.cleanWaitingConnectList(wd -> netEx);
                ci.future.completeExceptionally(netEx);
                it.remove();
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
        wakeup();
        return prepareStopFuture;
    }

    @Override
    public void doStop(DtTime timeout, boolean force) {
        wakeup();
    }

    protected void logWorkerStatus() {
        log.info("worker={}, outgoingConnects={}, pendingOutgoingRequests={}",
                workerName, outgoingConnects == null ? 0 : outgoingConnects.size(), workerStatus.pendingReqSize());
    }
}
