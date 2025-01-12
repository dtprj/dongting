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
import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.Timestamp;
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
@SuppressWarnings("Convert2Diamond")
class NioWorker extends AbstractLifeCircle implements Runnable {
    private static final DtLog log = DtLogs.getLogger(NioWorker.class);

    static long incomingConnectTimeout = 5 * 1000 * 1000 * 1000L;

    private final String workerName;
    private final Thread thread;
    private final NioStatus nioStatus;
    private final NioConfig config;
    private final NioClient client;
    private Selector selector;
    private final AtomicInteger notified = new AtomicInteger(0);

    private final CompletableFuture<Void> prepareStopFuture = new CompletableFuture<>();

    private int channelIndex;
    private final ArrayList<DtChannelImpl> channelsList;// client side only
    private final IntObjMap<DtChannelImpl> channels;
    private final IoWorkerQueue ioWorkerQueue;

    private final Timestamp timestamp = new Timestamp();

    private final LinkedList<DtChannelImpl> incomingConnects;//server side only
    private final LinkedList<ConnectInfo> outgoingConnects;//client side only
    final LongObjMap<WriteData> pendingOutgoingRequests = new LongObjMap<>();

    private final ByteBufferPool directPool;
    private final ByteBufferPool heapPool;

    final WorkerStatus workerStatus;

    private ByteBuffer readBuffer;
    private long readBufferUseTime;

    private final long cleanIntervalNanos;
    private long lastCleanNanos;

    private final PerfCallback perfCallback;

    private final ArrayList<Pair<Long, WriteData>> tempSortList = new ArrayList<>();

    public NioWorker(NioStatus nioStatus, String workerName, NioConfig config, NioClient client) {
        this.nioStatus = nioStatus;
        this.config = config;
        this.client = client;
        this.thread = new Thread(this, workerName);
        this.workerName = workerName;
        this.cleanIntervalNanos = config.getCleanInterval() * 1000 * 1000;
        this.perfCallback = config.getPerfCallback();

        this.channels = new IntObjMap<>();
        this.ioWorkerQueue = new IoWorkerQueue(this, config);
        if (client == null) {
            this.channelsList = null;
            this.outgoingConnects = null;
            this.incomingConnects = new LinkedList<>();
        } else {
            this.channelsList = new ArrayList<>();
            this.outgoingConnects = new LinkedList<>();
            this.incomingConnects = null;
        }

        this.directPool = config.getPoolFactory().createPool(timestamp, true);
        this.heapPool = config.getPoolFactory().createPool(timestamp, false);

        ByteBufferPool releaseSafePool = createReleaseSafePool((TwoLevelPool) heapPool, ioWorkerQueue);
        RefBufferFactory refBufferFactory = new RefBufferFactory(releaseSafePool, 800);

        workerStatus = new WorkerStatus(this);
        workerStatus.setIoQueue(ioWorkerQueue);
        workerStatus.setPendingRequests(pendingOutgoingRequests);
        workerStatus.setWakeupRunnable(this::wakeup);
        workerStatus.setDirectPool(directPool);
        workerStatus.setHeapPool(refBufferFactory);
        workerStatus.setTs(timestamp);
    }

    private ByteBufferPool createReleaseSafePool(TwoLevelPool heapPool, IoWorkerQueue ioWorkerQueue) {
        Consumer<ByteBuffer> callback = (buf) -> {
            boolean b = ioWorkerQueue.scheduleFromBizThread(() -> heapPool.getSmallPool().release(buf));
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
            ioWorkerQueue.dispatchActions();
            selector.close();

            cleanPendingOutgoingRequests(null, 3);

            List<DtChannelImpl> tempList = new ArrayList<>(channels.size());
            // can't modify channels map in foreach
            channels.forEach((index, dtc) -> {
                tempList.add(dtc);
            });
            for (DtChannelImpl dtc : tempList) {
                close(dtc);
            }

            if (client != null) {
                cleanOutgoingTimeoutConnect(ts);
            }
            if (readBuffer != null) {
                releaseReadBuffer();
            }

            config.getPoolFactory().destroyPool(directPool);
            config.getPoolFactory().destroyPool(heapPool);

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
                if (workerStatus.getPacketsToWrite() == 0 && pendingOutgoingRequests.size() == 0) {
                    prepareStopFuture.complete(null);
                }
            }
            if (ts.getNanoTime() - lastCleanNanos > cleanIntervalNanos || cleanIntervalNanos <= 0) {
                if (readBuffer != null && ts.getNanoTime() - readBufferUseTime > cleanIntervalNanos) {
                    releaseReadBuffer();
                }
                cleanTimeoutReq();
                if (client != null) {
                    cleanOutgoingTimeoutConnect(ts);
                } else {
                    cleanIncomingConnects(ts);
                }
                if (status == STATUS_RUNNING) {
                    tryReconnect(ts);
                }
                directPool.clean();
                heapPool.clean();
                lastCleanNanos = ts.getNanoTime();
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

    private int compare(Pair<Long, ?> a, Pair<Long, ?> b) {
        // we only need keep order in same connection, so we only use lower 32 bits.
        // the lower 32 bits may overflow, so we use subtraction to compare, can't use < or >
        return a.getLeft().intValue() - b.getLeft().intValue();
    }

    private boolean sel(Selector selector, Timestamp ts) {
        PerfCallback c = perfCallback;
        long start = c.takeTime(PerfConsts.RPC_D_WORKER_SEL, ts);
        try {
            long selectTimeoutMillis = config.getSelectTimeout();
            if (selectTimeoutMillis > 0) {
                selector.select(selectTimeoutMillis);
            } else {
                // for unit test find more problem
                selector.select();
            }
            return true;
        } catch (Exception e) {
            log.error("select failed: {}", workerName, e);
            return false;
        } finally {
            notified.lazySet(0);
            if (c.accept(PerfConsts.RPC_D_WORKER_WORK) || c.accept(PerfConsts.RPC_D_WORKER_SEL)) {
                perfCallback.refresh(ts);
            } else {
                ts.refresh(1);
            }
            c.fireTime(PerfConsts.RPC_D_WORKER_SEL, start, 1, 0, ts);
        }
    }

    private void prepareReadBuffer(Timestamp roundTime) {
        if (readBuffer == null) {
            readBuffer = directPool.borrow(config.getReadBufferSize());
        }
        readBuffer.clear();
        readBufferUseTime = roundTime.getNanoTime();
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
                long startTime = perfCallback.takeTime(PerfConsts.RPC_D_READ);
                int readBytes = sc.read(readBuffer);
                if (readBytes == -1) {
                    // log.info("socket read to end, remove it: {}", key.channel());
                    closeChannelBySelKey(key);
                    return;
                }
                perfCallback.fireTime(PerfConsts.RPC_D_READ, startTime, 1, readBytes);
                readBuffer.flip();
                dtc.afterRead(status == STATUS_RUNNING, readBuffer);
            }
            stage = "process socket write";
            if (key.isWritable()) {
                IoChannelQueue subQueue = dtc.getSubQueue();
                ByteBuffer buf = subQueue.getWriteBuffer(roundTime);
                if (buf != null) {
                    subQueue.setWriting(true);
                    long startTime = perfCallback.takeTime(PerfConsts.RPC_D_WRITE);
                    int x1 = buf.remaining();
                    sc.write(buf);
                    int x2 = buf.remaining();
                    if (x2 == 0) {
                        subQueue.afterBufferWriteFinish();
                    }
                    perfCallback.fireTime(PerfConsts.RPC_D_WRITE, startTime, 1, x2 - x1);
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

    private void wakeup() {
        if (Thread.currentThread() == thread) {
            return;
        }
        if (notified.incrementAndGet() == 1) {
            selector.wakeup();
        }
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
        dtc.getSubQueue().setRegisterForWrite(new RegWriteRunner(selectionKey));

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
        boolean autoRetry;
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
            channels.put(dtc.getChannelIndexInWorker(), dtc);
            incomingConnects.addLast(dtc);
        }, null);
    }

    void finishHandshake(DtChannelImpl dtc) {
        dtc.handshake = true;
        if (incomingConnects != null) {
            //server side
            incomingConnects.remove(dtc);
        }
        if (nioStatus.channelListener != null) {
            try {
                nioStatus.channelListener.onConnected(dtc);
            } catch (Throwable e) {
                log.error("channelListener.onConnected error: {}", e);
            } finally {
                dtc.listenerOnConnectedCalled = true;
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

    void doConnect(CompletableFuture<Void> f, Peer peer, DtTime deadline, boolean autoRetry) {
        if (status >= STATUS_PREPARE_STOP) {
            f.completeExceptionally(new NetException("worker closed"));
            return;
        }
        PeerStatus s = peer.getStatus();
        if (s == PeerStatus.not_connect) {
            peer.autoReconnect = true;
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
            HostPort hp = peer.getEndPoint();
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
            ci.autoRetry = autoRetry;
            outgoingConnects.add(ci);
            peer.connectInfo = ci;

            sc.register(selector, SelectionKey.OP_CONNECT, ci);
            sc.connect(addr);
        } catch (Throwable e) {
            if (sc != null) {
                closeChannel0(sc);
            }
            peer.markNotConnect(config, workerStatus, autoRetry);
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
            channels.put(dtc.getChannelIndexInWorker(), dtc);
            channelsList.add(dtc);
        } catch (Throwable e) {
            log.warn("connect channel fail: {}, {}", ci.peer.getEndPoint(), e.toString());
            closeChannel0(channel);
            ci.peer.markNotConnect(config, workerStatus, ci.autoRetry);
            ci.peer.connectInfo = null;
            NetException netEx = new NetException(e);
            ci.peer.cleanWaitingConnectList(wd -> netEx);
            ci.future.completeExceptionally(netEx);
            return;
        }

        Peer peer = ci.peer;
        peer.dtChannel = dtc;
        peer.connectionId++;
        peer.status = PeerStatus.handshake;

        try {
            sendHandshake(dtc, ci);
        } catch (Throwable e) {
            log.error("send handshake fail: {}", e);
            close(dtc);
        }
    }

    private void sendHandshake(DtChannelImpl dtc, ConnectInfo ci) {
        RpcCallback<HandshakeBody> rpcCallback = new RpcCallback<HandshakeBody>() {
            @Override
            public void success(ReadPacket<HandshakeBody> resp) {
                handshakeSuccess(dtc, ci, resp);
            }

            @Override
            public void fail(Throwable ex) {
                log.error("handshake fail: {}", ex);
                close(dtc);
            }
        };

        HandshakeBody hb = new HandshakeBody();
        hb.majorVersion = DtUtil.RPC_MAJOR_VER;
        hb.minorVersion = DtUtil.RPC_MINOR_VER;
        ConfigBody cb = new ConfigBody();
        cb.maxPacketSize = config.getMaxPacketSize();
        cb.maxBodySize = config.getMaxBodySize();
        cb.maxOutPending = config.getMaxOutRequests();
        cb.maxOutPendingBytes = config.getMaxOutBytes();
        cb.maxInPending = config.getMaxInRequests();
        cb.maxInPendingBytes = config.getMaxInBytes();
        hb.config = cb;
        HandshakeBody.WritePacket p = new HandshakeBody.WritePacket(hb);
        p.packetType = PacketType.TYPE_REQ;
        p.command = Commands.CMD_HANDSHAKE;

        WriteData wd = new WriteData(dtc, p, ci.deadline, rpcCallback,
                ctx -> ctx.toDecoderCallback(new HandshakeBody.Callback()));
        dtc.getSubQueue().enqueue(wd);
        // send pending request as quickly as possible, even before handshake finished
        ci.peer.enqueueAfterConnect();
    }

    private void handshakeSuccess(DtChannelImpl dtc, ConnectInfo ci, ReadPacket<HandshakeBody> resp) {
        if (status >= STATUS_PREPARE_STOP) {
            log.info("handshake while worker closed, ignore it: {}", dtc.getChannel());
            close(dtc);
            return;
        }
        if (resp.getBody() == null || resp.getBody().config == null) {
            log.error("handshake fail: invalid response");
            close(dtc);
            return;
        }
        ConfigBody cb = resp.getBody().config;
        if (config != null && config.isServerHint()) {
            client.processServerConfigHint(dtc.peer, cb);
            config.writeFence();
        }

        ci.peer.status = PeerStatus.connected;
        ci.peer.resetConnectRetry(workerStatus);
        finishHandshake(dtc);
        ci.future.complete(null);
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
            peer.autoReconnect = false;
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

    // 1 clean by dtc, 2 clean by timeout, 3 clean all
    private void cleanPendingOutgoingRequests(DtChannelImpl dtc, int mode) {
        ArrayList<Pair<Long, WriteData>> list;
        if (mode == 1) {
            // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
            // use new array list to avoid ConcurrentModificationException
            list = new ArrayList<>();
        } else {
            list = this.tempSortList;
        }
        try {
            this.pendingOutgoingRequests.forEach((key, wd) -> {
                if (mode == 1) {
                    if (wd.dtc == dtc) {
                        if (wd.callback != null) {
                            list.add(new Pair<>(key, wd));
                        }
                        return false;
                    } else {
                        return true;
                    }
                } else if (mode == 2) {
                    DtTime t = wd.getTimeout();
                    if (wd.getDtc().isClosed() || t.isTimeout(timestamp)) {
                        if (wd.callback != null) {
                            list.add(new Pair<>(key, wd));
                        }
                        return false;
                    } else {
                        return true;
                    }
                } else {
                    if (wd.callback != null) {
                        list.add(new Pair<>(key, wd));
                    }
                    return false;
                }
            });

            if (list.isEmpty()) {
                return;
            }
            list.sort(this::compare);
            for (Pair<Long, WriteData> p : list) {
                WriteData wd = p.getRight();
                // callFail may cause sendHeartbeat callback call close(dtc), and clean pendingOutgoingRequests.
                // so callFail should be idempotent
                if (mode == 1) {
                    wd.callFail(false, new NetException("channel closed, cancel pending request in NioWorker"));
                } else if (mode == 2) {
                    DtTime t = wd.getTimeout();
                    if (wd.getDtc().isClosed()) {
                        wd.callFail(false, new NetException("channel closed, future cancelled by timeout cleaner"));
                    } else if (t.isTimeout(timestamp)) {
                        log.debug("drop timeout request: {}ms, seq={}, {}", t.getTimeout(TimeUnit.MILLISECONDS),
                                wd.getData().getSeq(), wd.getDtc().getChannel());
                        String msg = "request is timeout: " + t.getTimeout(TimeUnit.MILLISECONDS) + "ms";
                        wd.callFail(false, new NetTimeoutException(msg));
                    }
                } else {
                    wd.callFail(false, new NetException("client closed"));
                }
            }
        } finally {
            list.clear();
        }
    }

    void close(DtChannelImpl dtc) {
        if (dtc.isClosed()) {
            return;
        }

        dtc.close();

        if (nioStatus.channelListener != null && dtc.listenerOnConnectedCalled) {
            try {
                nioStatus.channelListener.onDisconnected(dtc);
            } catch (Throwable e) {
                log.error("channelListener.onDisconnected error: {}", e);
            }
        }

        Peer peer = dtc.peer;
        if (peer != null) {
            peer.dtChannel = null;
            peer.markNotConnect(config, workerStatus, false);
        }
        channels.remove(dtc.getChannelIndexInWorker());
        if (channelsList != null) {
            // O(n) in client side
            channelsList.remove(dtc);
        }
        if (incomingConnects != null) {
            incomingConnects.remove(dtc);
        }
        closeChannel0(dtc.getChannel());
        if (config.isFinishPendingImmediatelyWhenChannelClose()) {
            cleanPendingOutgoingRequests(dtc, 1);
        }
        dtc.getSubQueue().cleanChannelQueue();
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

    private void cleanTimeoutReq() {
        cleanPendingOutgoingRequests(null, 2);

        if (client != null) {
            client.cleanWaitConnectReq(wd -> {
                if (wd.getTimeout().isTimeout(timestamp)) {
                    return new NetTimeoutException("wait connect timeout");
                }
                return null;
            });
        }
    }

    private void cleanOutgoingTimeoutConnect(Timestamp roundStartTime) {
        boolean close = status >= STATUS_PREPARE_STOP;
        for (Iterator<ConnectInfo> it = this.outgoingConnects.iterator(); it.hasNext(); ) {
            ConnectInfo ci = it.next();
            if (close || ci.deadline.isTimeout(roundStartTime)) {
                ci.peer.markNotConnect(config, workerStatus, ci.autoRetry);
                ci.peer.connectInfo = null;
                NetException netEx;
                if (close) {
                    log.info("worker closed, connect fail: {}", ci.peer.getEndPoint());
                    netEx = new NetException("worker closed");
                } else {
                    log.warn("connect timeout: {}ms, {}", ci.deadline.getTimeout(TimeUnit.MILLISECONDS),
                            ci.peer.getEndPoint());
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
        long t = roundStartTime.getNanoTime() - incomingConnectTimeout;
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
        if (client == null || workerStatus.retryConnect <= 0) {
            return;
        }
        List<Peer> peers = client.getPeers();
        for (Peer p : peers) {
            if (!p.autoReconnect || p.status != PeerStatus.not_connect) {
                continue;
            }
            if (ts.getNanoTime() - p.lastRetryNanos > 0) {
                CompletableFuture<Void> f = new CompletableFuture<>();
                DtTime deadline = new DtTime(5, TimeUnit.SECONDS);
                doConnect(f, p, deadline, true);
            }
        }
    }

    // invoke by other threads
    void writeReqInBizThreads(WriteData data) {
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

    public Thread getThread() {
        return thread;
    }

    public boolean isServer() {
        return client == null;
    }

    public ArrayList<DtChannelImpl> getChannelsList() {
        return channelsList;
    }

    protected void logWorkerStatus() {
        log.info("worker={}, outgoingConnects={}, pendingOutgoingRequests={}",
                workerName, outgoingConnects == null ? 0 : outgoingConnects.size(), pendingOutgoingRequests.size());
    }
}
