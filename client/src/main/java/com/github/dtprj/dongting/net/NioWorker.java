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
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtThread;
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
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * each worker represent a thread.
 *
 * @author huangli
 */
class NioWorker extends AbstractLifeCircle implements Runnable {
    private static final DtLog log = DtLogs.getLogger(NioWorker.class);

    private final String workerName;
    private final DtThread thread;
    private final NioStatus nioStatus;
    private final NioConfig config;
    private final NioClient client;
    private Selector selector;
    private final AtomicInteger notified = new AtomicInteger(0);

    private int channelIndex;
    private final ArrayList<DtChannel> channelsList;
    private final IntObjMap<DtChannel> channels;
    private final IoWorkerQueue ioWorkerQueue;

    private final Timestamp timestamp = new Timestamp();

    private final LinkedList<ConnectInfo> outgoingConnects = new LinkedList<>();
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
        this.thread = new DtThread(this, workerName);
        this.workerName = workerName;
        this.cleanIntervalNanos = config.getCleanInterval() * 1000 * 1000;
        this.perfCallback = config.getPerfCallback();

        this.channels = new IntObjMap<>();
        this.ioWorkerQueue = new IoWorkerQueue(this, config);
        if (client == null) {
            this.channelsList = null;
        } else {
            this.channelsList = new ArrayList<>();
        }

        this.directPool = config.getPoolFactory().createPool(timestamp, true);
        this.heapPool = config.getPoolFactory().createPool(timestamp, false);

        workerStatus = new WorkerStatus();
        workerStatus.setIoQueue(ioWorkerQueue);
        workerStatus.setPendingRequests(pendingOutgoingRequests);
        workerStatus.setWakeupRunnable(this::wakeup);
        workerStatus.setDirectPool(directPool);
        workerStatus.setHeapPool(heapPool);
        workerStatus.setTs(timestamp);
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

            finishPendingReq();

            List<DtChannel> tempList = new ArrayList<>(channels.size());
            // can't modify channels map in foreach
            channels.forEach((index, dtc) -> {
                tempList.add(dtc);
            });
            for (DtChannel dtc : tempList) {
                close(dtc);
            }

            if (client != null) {
                client.cleanWaitConnectReq(null);
            }
            if (readBuffer != null) {
                releaseReadBuffer();
            }

            config.getPoolFactory().destroyPool(directPool);
            config.getPoolFactory().destroyPool(heapPool);

            log.info("worker thread [{}] finished.");
            if (log.isDebugEnabled()) {
                log.debug("direct pool stat: {}\nheap pool stat: {}", directPool.formatStat(), heapPool.formatStat());
            } else if (DtUtil.DEBUG) {
                log.info("direct pool stat: {}\nheap pool stat: {}", directPool.formatStat(), heapPool.formatStat());
            }
        } catch (Exception e) {
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
            if (status == STATUS_PREPARE_STOP) {
                ioWorkerQueue.dispatchActions();
                if (workerStatus.getFramesToWrite() == 0 && pendingOutgoingRequests.size() == 0) {
                    prepareStopFuture.complete(null);
                }
            }
            if (ts.getNanoTime() - lastCleanNanos > cleanIntervalNanos || cleanIntervalNanos <= 0) {
                if (readBuffer != null && ts.getNanoTime() - readBufferUseTime > cleanIntervalNanos) {
                    releaseReadBuffer();
                }
                // TODO shrink channels map if the it's internal array is too large
                cleanTimeoutReq(ts);
                cleanTimeoutConnect(ts);
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
        c.callDuration(PerfConsts.RPC_D_WORKER_WORK, startTime, 0, ts);
    }

    private int compare(Pair<Long, ?> a, Pair<Long, ?> b) {
        // we only need keep order in same connection, so we only use lower 32 bits.
        // the lower 32 bits may overflow, so we use subtraction to compare, can't use < or >
        return a.getLeft().intValue() - b.getLeft().intValue();
    }

    private void finishPendingReq() {
        try {
            pendingOutgoingRequests.forEach((key, value) -> {
                if (value.getFuture() != null) {
                    tempSortList.add(new Pair<>(key, value));
                }
                // return false to remove it
                return false;
            });
            // sort to keep fail order
            tempSortList.sort(this::compare);
            for (Pair<Long, WriteData> p : tempSortList) {
                p.getRight().getFuture().completeExceptionally(new NetException("client closed"));
            }
        } finally {
            tempSortList.clear();
        }
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
            c.callDuration(PerfConsts.RPC_D_WORKER_SEL, start, 0, ts);
        }
    }

    private void prepareReadBuffer(Timestamp roundTime) {
        if (readBuffer == null) {
            readBuffer = directPool.borrow(config.getReadBufferSize());
            // change to little endian since protobuf is little endian
            readBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        readBuffer.clear();
        readBufferUseTime = roundTime.getNanoTime();
    }

    private void releaseReadBuffer() {
        // recover to big endian
        readBuffer.order(ByteOrder.BIG_ENDIAN);
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
            DtChannel dtc = (DtChannel) key.attachment();
            if (key.isReadable()) {
                prepareReadBuffer(roundTime);
                long startTime = perfCallback.takeTime(PerfConsts.RPC_D_READ);
                int readCount = sc.read(readBuffer);
                if (readCount == -1) {
                    // log.info("socket read to end, remove it: {}", key.channel());
                    closeChannelBySelKey(key);
                    return;
                }
                perfCallback.callDuration(PerfConsts.RPC_D_READ, startTime, readCount);
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
                    int x = buf.remaining();
                    sc.write(buf);
                    x = x - buf.remaining();
                    perfCallback.callDuration(PerfConsts.RPC_D_WRITE, startTime, x);
                } else {
                    // no data to write
                    subQueue.setWriting(false);
                    key.interestOps(SelectionKey.OP_READ);
                    perfCallback.callCount(PerfConsts.RPC_C_MARK_READ);
                }
            }
        } catch (Exception e) {
            if (e instanceof IOException) {
                log.warn("{} error, channel will close: {}", stage, e.toString());
            } else {
                log.warn("{} error, channel will close: {}", stage, e);
            }
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

    private DtChannel initNewChannel(SocketChannel sc, Peer peer) throws IOException {
        sc.configureBlocking(false);
        sc.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
        sc.setOption(StandardSocketOptions.TCP_NODELAY, true);

        // 32 bit channelIndex may overflow, use channels map to keep it unique in every worker
        while (channels.get(channelIndex) != null) {
            channelIndex++;
        }
        DtChannel dtc = new DtChannel(nioStatus, workerStatus, config, sc, channelIndex++);
        SelectionKey selectionKey = sc.register(selector, SelectionKey.OP_READ, dtc);
        dtc.getSubQueue().setRegisterForWrite(new RegWriteRunner(selectionKey));

        if (peer != null) {
            peer.setDtChannel(dtc);
            dtc.setPeer(peer);
            peer.setConnectionId(peer.getConnectionId() + 1);
            peer.setStatus(PeerStatus.connected);
        }

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
            perfCallback.callCount(PerfConsts.RPC_C_MARK_WRITE);
        }
    }

    static class ConnectInfo {
        CompletableFuture<Void> future;
        Peer peer;
        SocketChannel channel;
        DtTime deadline;
    }

    // invoke by NioServer accept thead
    public void newChannelAccept(SocketChannel sc) {
        doInIoThread(() -> {
            try {
                DtChannel dtc = initNewChannel(sc, null);
                // TODO do handshake
                channels.put(dtc.getChannelIndexInWorker(), dtc);
            } catch (Throwable e) {
                log.warn("accept channel fail: {}, {}", sc, e.toString());
                closeChannel0(sc);
            }
        }, null);
    }

    // invoke by other threads
    public CompletableFuture<Void> connect(Peer peer, DtTime deadline) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        if (status >= STATUS_PREPARE_STOP) {
            f.completeExceptionally(new NetException("worker closed"));
        } else {
            doInIoThread(() -> doConnect(f, peer, deadline), f);
        }
        return f;
    }

    void doConnect(CompletableFuture<Void> f, Peer peer, DtTime deadline) {
        PeerStatus s = peer.getStatus();
        if (s == PeerStatus.not_connect) {
            peer.setStatus(PeerStatus.connecting);
        } else if (s == PeerStatus.removed) {
            NetException ex = new NetException("peer is removed");
            f.completeExceptionally(ex);
            return;
        } else if (s == PeerStatus.connected) {
            f.complete(null);
            return;
        } else {
            peer.getConnectInfo().future.whenComplete((v, ex) -> {
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
            outgoingConnects.add(ci);
            peer.setConnectInfo(ci);

            sc.register(selector, SelectionKey.OP_CONNECT, ci);
            sc.connect(addr);
        } catch (Throwable e) {
            if (sc != null) {
                closeChannel0(sc);
            }
            peer.setStatus(PeerStatus.not_connect);
            peer.setConnectInfo(null);
            NetException netEx = new NetException(e);
            peer.cleanWaitingConnectList(wd -> netEx);
            f.completeExceptionally(netEx);
        }
    }

    // called client side
    private void whenConnected(SelectionKey key) {
        ConnectInfo ci = (ConnectInfo) key.attachment();
        SocketChannel channel = ci.channel;
        try {
            if (!ci.future.isDone()) {
                channel.finishConnect();
                DtChannel dtc = initNewChannel(channel, ci.peer);
                channels.put(dtc.getChannelIndexInWorker(), dtc);
                channelsList.add(dtc);
                ci.peer.enqueueAfterConnect();
                ci.future.complete(null);
            }
        } catch (Throwable e) {
            log.warn("connect channel fail: {}, {}", ci.peer.getEndPoint(), e.toString());
            closeChannel0(channel);
            ci.peer.setStatus(PeerStatus.not_connect);
            ci.peer.setConnectInfo(null);
            NetException netEx = new NetException(e);
            ci.peer.cleanWaitingConnectList(wd -> netEx);
            ci.future.completeExceptionally(netEx);
        } finally {
            outgoingConnects.remove(ci);
        }
    }

    public void doInIoThread(Runnable runnable, CompletableFuture<?> future) {
        try {
            ioWorkerQueue.scheduleFromBizThread(runnable);
            wakeup();
        } catch (NetException e) {
            if (future != null) {
                future.completeExceptionally(e);
            }
        }
    }

    public CompletableFuture<Void> disconnect(Peer peer) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        doInIoThread(() -> {
            if (peer.getDtChannel() == null) {
                f.complete(null);
                return;
            }
            try {
                close(peer.getDtChannel());
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
        if (obj instanceof DtChannel) {
            DtChannel dtc = (DtChannel) obj;
            close(dtc);
        } else {
            BugLog.getLog().error("assert false. key attachment is not " + DtChannel.class.getName());
            closeChannel0(sc);
            assert false;
        }
    }

    void close(DtChannel dtc) {
        if (dtc.isClosed()) {
            return;
        }

        dtc.close();
        Peer peer = dtc.getPeer();
        if (peer != null && peer.getDtChannel() == dtc) {
            peer.setDtChannel(null);
            peer.setStatus(PeerStatus.not_connect);
        }
        channels.remove(dtc.getChannelIndexInWorker());
        if (channelsList != null) {
            // O(n) in client side
            channelsList.remove(dtc);
        }
        closeChannel0(dtc.getChannel());
        if (config.isFinishPendingImmediatelyWhenChannelClose()) {
            try {
                pendingOutgoingRequests.forEach((key, wd) -> {
                    if (wd.getDtc() == dtc) {
                        if (wd.getFuture() != null) {
                            tempSortList.add(new Pair<>(key, wd));
                        }
                        return false;
                    }
                    return true;
                });
                // sort to keep fail order
                tempSortList.sort(this::compare);
                for (Pair<Long, WriteData> p : tempSortList) {
                    p.getRight().getFuture().completeExceptionally(new NetException(
                            "channel closed, cancel pending request in NioWorker"));
                }
            } finally {
                tempSortList.clear();
            }
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

    private void cleanTimeoutReq(Timestamp roundStartTime) {
        try {
            this.pendingOutgoingRequests.forEach((key, wd) -> {
                DtTime t = wd.getTimeout();
                if (wd.getDtc().isClosed() || t.isTimeout(roundStartTime)) {
                    tempSortList.add(new Pair<>(key, wd));
                    return false;
                }
                return true;
            });

            tempSortList.sort(this::compare);
            for (Pair<Long, WriteData> p : tempSortList) {
                WriteData wd = p.getRight();
                DtTime t = wd.getTimeout();
                if (wd.getDtc().isClosed()) {
                    if (wd.getFuture() != null) {
                        wd.getFuture().completeExceptionally(new NetException("channel closed, future cancelled by timeout cleaner"));
                    }
                } else if (t.isTimeout(roundStartTime)) {
                    log.debug("drop timeout request: {}ms, seq={}, {}",
                            t.getTimeout(TimeUnit.MILLISECONDS), wd.getData().getSeq(),
                            wd.getDtc().getChannel());
                    if (wd.getFuture() != null) {
                        String msg = "request is timeout: " + t.getTimeout(TimeUnit.MILLISECONDS) + "ms";
                        wd.getFuture().completeExceptionally(new NetTimeoutException(msg));
                    }
                }
            }
        } finally {
            tempSortList.clear();
        }

        if (client != null) {
            client.cleanWaitConnectReq(wd -> {
                if (wd.getTimeout().isTimeout(timestamp)) {
                    return new NetTimeoutException("wait connect timeout");
                }
                return null;
            });
        }
    }

    private void cleanTimeoutConnect(Timestamp roundStartTime) {
        for (Iterator<ConnectInfo> it = this.outgoingConnects.iterator(); it.hasNext(); ) {
            ConnectInfo ci = it.next();
            if (ci.deadline.isTimeout(roundStartTime)) {
                ci.peer.setStatus(PeerStatus.not_connect);
                ci.peer.setConnectInfo(null);
                log.warn("connect timeout: {}ms, {}", ci.deadline.getTimeout(TimeUnit.MILLISECONDS),
                        ci.peer.getEndPoint());
                closeChannel0(ci.channel);
                NetTimeoutException netEx = new NetTimeoutException("connect timeout");
                ci.peer.cleanWaitingConnectList(wd -> netEx);
                ci.future.completeExceptionally(netEx);
                it.remove();
            }
        }
    }

    // invoke by other threads
    public void writeReqInBizThreads(Peer peer, WriteFrame frame, Decoder<?> decoder,
                                     DtTime timeout, CompletableFuture<ReadFrame<?>> future) {
        Objects.requireNonNull(timeout);
        Objects.requireNonNull(future);

        WriteData data = new WriteData(peer, frame, timeout, future, decoder);
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
    protected CompletableFuture<Void> prepareStop() {
        CompletableFuture<Void> f = super.prepareStop();
        wakeup();
        return f;
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

    public ArrayList<DtChannel> getChannelsList() {
        return channelsList;
    }

    protected void logWorkerStatus() {
        log.info("worker={}, outgoingConnects={}, pendingOutgoingRequests={}",
                workerName, outgoingConnects.size(), pendingOutgoingRequests.size());
    }
}
