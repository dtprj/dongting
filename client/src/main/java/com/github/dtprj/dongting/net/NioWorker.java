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

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * each worker represent a thread.
 *
 * @author huangli
 */
class NioWorker extends AbstractLifeCircle implements Runnable {
    private static final DtLog log = DtLogs.getLogger(NioWorker.class);
    private static final int SS_RUNNING = 0;
    private static final int SS_PRE_STOP = 1;
    private static final int SS_STOP = 2;

    private final String workerName;
    private final Thread thread;
    private final NioStatus nioStatus;
    private final NioConfig config;
    private volatile int stopStatus = SS_RUNNING;
    private Selector selector;
    private final AtomicBoolean notified = new AtomicBoolean(false);

    private final ConcurrentLinkedQueue<Runnable> actions = new ConcurrentLinkedQueue<>();

    private int channelIndex;
    private final Collection<DtChannel> channels;
    private final IoQueue ioQueue;

    private final LongObjectHashMap<WriteData> pendingOutgoingRequests = new LongObjectHashMap<>();
    private final CompletableFuture<Void> preCloseFuture = new CompletableFuture<>();

    private final Semaphore requestSemaphore;

    private final ByteBufferPool pool;

    private final WorkerParams workerParams;

    private ByteBuffer readBuffer;
    private long readBufferUseTime;
    private final long readBufferTimeoutNanos;

    public NioWorker(NioStatus nioStatus, String workerName, NioConfig config) {
        this.nioStatus = nioStatus;
        this.config = config;
        this.thread = new Thread(this);
        this.workerName = workerName;
        this.thread.setName(workerName);
        this.requestSemaphore = nioStatus.getRequestSemaphore();
        this.readBufferTimeoutNanos = config.getReadBufferTimeoutMillis() * 1000 * 1000;

        if (config instanceof NioServerConfig) {
            this.channels = new HashSet<>();
            this.ioQueue = new IoQueue(null);
        } else {
            this.channels = new ArrayList<>();
            this.ioQueue = new IoQueue((ArrayList<DtChannel>) channels);
        }

        this.pool = new ByteBufferPool(true, config.getBufPoolSize(), config.getBufPoolMinCount(),
                config.getBufPoolMaxCount(), config.getBufPoolTimeout());

        workerParams = new WorkerParams();
        workerParams.setIoQueue(ioQueue);
        workerParams.setPendingRequests(pendingOutgoingRequests);
        workerParams.setWakeupRunnable(this::wakeup);
        workerParams.setPool(pool);
    }

    // invoke by NioServer accept thead
    public void newChannelAccept(SocketChannel sc) {
        actions.add(() -> {
            try {
                DtChannel dtc = initNewChannel(sc, null);
                channels.add(dtc);
            } catch (Exception e) {
                log.warn("accept channel fail: {}, {}", sc, e.toString());
                closeChannel0(sc);
            }
        });
        wakeup();
    }

    // invoke by other threads
    public CompletableFuture<Void> connect(Peer peer) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        actions.add(() -> doConnect(f, peer));
        wakeup();
        return f;
    }

    @Override
    public void run() {
        long cleanIntervalNanos = config.getCleanIntervalMills() * 1000 * 1000;
        long lastCleanNano = System.nanoTime();
        long selectTimeoutMillis = config.getSelectTimeoutMillis();
        Selector selector = this.selector;
        int stopStatus;
        while ((stopStatus = this.stopStatus) <= SS_PRE_STOP) {
            try {
                long roundStartTime = run0(selector, selectTimeoutMillis, stopStatus);
                if (roundStartTime - lastCleanNano > cleanIntervalNanos) {
                    cleanTimeoutReq();
                    pool.clean(roundStartTime);
                    lastCleanNano = roundStartTime;
                }
            } catch (Throwable e) {
                log.error("", e);
            }
        }
        try {
            selector.close();
            for (DtChannel dtc : channels) {
                closeChannel0(dtc.getChannel());
            }
            log.info("worker thread finished: {}", workerName);
        } catch (Exception e) {
            log.error("close error. {}", e);
        }
    }

    private long run0(Selector selector, long selectTimeoutMillis, int stopStatus) {
        if (!select(selector, selectTimeoutMillis)) {
            return System.nanoTime();
        }
        long roundTime = System.nanoTime();
        performActions();
        boolean hasDataToWrite = ioQueue.dispatchWriteQueue(pendingOutgoingRequests);
        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            hasDataToWrite |= processOneSelectionKey(key, stopStatus, roundTime);
            iterator.remove();
        }
        cleanReadBuffer(roundTime);
        if (stopStatus == SS_PRE_STOP) {
            if (!hasDataToWrite && pendingOutgoingRequests.size() == 0) {
                preCloseFuture.complete(null);
            }
        }
        return roundTime;
    }

    private void prepareReadBuffer(long roundTime) {
        if (readBuffer == null) {
            readBuffer = pool.borrow(config.getReadBufferSize());
            // change to little endian since protobuf is little endian
            readBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        readBuffer.clear();
        readBufferUseTime = roundTime;
    }

    private void cleanReadBuffer(long roundTime) {
        if (readBuffer != null && roundTime - readBufferUseTime > readBufferTimeoutNanos) {
            // recover to big endian
            readBuffer.order(ByteOrder.BIG_ENDIAN);
            pool.release(readBuffer, roundTime);
            readBuffer = null;
            readBufferUseTime = 0;
        }
    }

    private boolean processOneSelectionKey(SelectionKey key, int stopStatus, long roundTime) {
        SocketChannel sc = (SocketChannel) key.channel();
        int stage = 0;
        boolean hasDataToWrite = false;
        try {
            if (!key.isValid()) {
                log.info("socket may closed, remove it: {}", key.channel());
                closeChannel(key);
                return false;
            }
            stage = 1;
            if (key.isConnectable()) {
                processConnect(key);
                return false;
            }

            stage = 2;
            DtChannel dtc = (DtChannel) key.attachment();
            if (key.isReadable()) {
                prepareReadBuffer(roundTime);
                int readCount = sc.read(readBuffer);
                if (readCount == -1) {
                    // log.info("socket read to end, remove it: {}", key.channel());
                    closeChannel(key);
                    return false;
                }
                readBuffer.flip();
                dtc.afterRead(stopStatus == SS_RUNNING, readBuffer);
            }
            stage = 3;
            if (key.isWritable()) {
                IoSubQueue subQueue = dtc.getSubQueue();
                ByteBuffer buf = subQueue.getWriteBuffer(roundTime);
                if (buf != null) {
                    hasDataToWrite = true;
                    subQueue.setWriting(true);
                    sc.write(buf);
                } else {
                    // no data to write
                    subQueue.setWriting(false);
                    key.interestOps(SelectionKey.OP_READ);
                }
            }
        } catch (Exception e) {
            switch (stage) {
                case 1:
                    log.warn("process socket connect error: {}", e.toString());
                    break;
                case 2:
                    log.warn("process socket read error: {}", e.toString());
                    break;
                case 3:
                    log.warn("process socket write error: {}", e.toString());
                    break;
                default:
                    log.warn("socket error: {}", e.toString());
            }
            closeChannel(key);
        }
        return hasDataToWrite;
    }

    private void closeChannel(SelectionKey key) {
        Object obj = key.attachment();
        SocketChannel sc = (SocketChannel) key.channel();
        if (obj instanceof DtChannel) {
            DtChannel dtc = (DtChannel) obj;
            if (!dtc.isClosed()) {
                dtc.close();
                channels.remove(dtc);
                closeChannel0(sc);
            }
        } else {
            channels.removeIf(d -> {
                if (d.getChannel() == sc) {
                    if (!d.isClosed()) {
                        d.close();
                        closeChannel0(sc);
                    }
                    return true;
                }
                return false;
            });
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

    private boolean select(Selector selector, long selectTimeoutMillis) {
        try {
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
            notified.set(false);
        }
    }

    private void wakeup() {
        if (Thread.currentThread() == thread) {
            return;
        }
        if (notified.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    private void performActions() {
        Runnable r;
        while ((r = actions.poll()) != null) {
            r.run();
        }
    }

    private DtChannel initNewChannel(SocketChannel sc, Peer peer) throws IOException {
        sc.configureBlocking(false);
        sc.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
        sc.setOption(StandardSocketOptions.TCP_NODELAY, true);

        DtChannel dtc = new DtChannel(nioStatus, workerParams, config, sc, channelIndex++);
        SelectionKey selectionKey = sc.register(selector, SelectionKey.OP_READ, dtc);
        dtc.getSubQueue().setRegisterForWrite(new RegWriteRunner(selectionKey));

        if (peer != null) {
            peer.setDtChannel(dtc);
            dtc.setPeer(peer);
        }

        log.info("new DtChannel init: {}", sc);
        return dtc;
    }

    private static class RegWriteRunner implements Runnable {
        SelectionKey key;
        RegWriteRunner(SelectionKey key) {
            this.key = key;
        }

        @Override
        public void run() {
            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }
    }

    private void doConnect(CompletableFuture<Void> f, Peer peer) {
        if (peer.getDtChannel() != null) {
            f.completeExceptionally(new NetException("peer connected"));
            return;
        }
        SocketChannel sc = null;
        try {
            HostPort hp = (HostPort) peer.getEndPoint();
            InetSocketAddress addr = new InetSocketAddress(hp.getHost(), hp.getPort());
            sc = SocketChannel.open();
            sc.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
            sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
            sc.configureBlocking(false);
            sc.register(selector, SelectionKey.OP_CONNECT, new Object[]{f, peer});
            sc.connect(addr);
        } catch (Throwable e) {
            if (sc != null) {
                closeChannel0(sc);
            }
            f.completeExceptionally(new NetException(e));
        }
    }

    private void processConnect(SelectionKey key) {
        Object[] att = (Object[]) key.attachment();
        @SuppressWarnings("unchecked")
        CompletableFuture<Void> f = (CompletableFuture<Void>) att[0];
        Peer peer = (Peer) att[1];
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            channel.finishConnect();
            DtChannel dtc = initNewChannel(channel, peer);
            channels.add(dtc);
            f.complete(null);
        } catch (Exception e) {
            log.warn("connect channel fail: {}, {}", channel, e.toString());
            closeChannel0((SocketChannel) key.channel());
            f.completeExceptionally(new NetException(e));
        }
    }

    private void cleanTimeoutReq() {
        LongObjectHashMap<WriteData> map = this.pendingOutgoingRequests;
        Iterator<LongObjectCursor<WriteData>> it = map.iterator();
        LinkedList<Long> expireList = null;
        while (it.hasNext()) {
            LongObjectCursor<WriteData> en = it.next();
            WriteData d = en.value;
            DtTime t = d.getTimeout();
            if (t.rest(TimeUnit.MILLISECONDS) <= 0) {
                if (expireList == null) {
                    expireList = new LinkedList<>();
                }
                expireList.add(en.key);
            }
        }
        if (expireList == null) {
            return;
        }
        for(Long key: expireList) {
            WriteData d = map.remove(key);
            DtTime t = d.getTimeout();
            log.debug("drop timeout request: {}ms, seq={}, {}",
                    t.getTimeout(TimeUnit.MILLISECONDS), d.getData().getSeq(),
                    d.getDtc());
            String msg = "timeout: " + t.getTimeout(TimeUnit.MILLISECONDS) + "ms";
            d.getFuture().completeExceptionally(new NetTimeoutException(msg));
            requestSemaphore.release();
        }
    }

    // invoke by other threads
    public void writeReqInBizThreads(Peer peer, WriteFrame frame, Decoder decoder,
                                      DtTime timeout, CompletableFuture<ReadFrame> future) {
        Objects.requireNonNull(timeout);
        Objects.requireNonNull(future);

        WriteData data = new WriteData(peer, frame, timeout, future, decoder);
        this.ioQueue.write(data);
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

    public void preStop() {
        stopStatus = SS_PRE_STOP;
        wakeup();
    }

    @Override
    public void doStop() {
        stopStatus = SS_STOP;
        wakeup();
    }

    public Thread getThread() {
        return thread;
    }

    public CompletableFuture<Void> getPreCloseFuture() {
        return preCloseFuture;
    }
}
