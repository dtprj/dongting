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

import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.common.LongObjMap;
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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
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

    private int statMarkReadCount;
    private int statMarkWriteCount;
    private int statWriteCount;
    private long statWriteBytes;
    private int statReadCount;
    private long statReadBytes;

    private final String workerName;
    private final Thread thread;
    private final NioStatus nioStatus;
    private final NioConfig config;
    private volatile int stopStatus = SS_RUNNING;
    private Selector selector;
    private final AtomicBoolean notified = new AtomicBoolean(false);

    private int channelIndex;
    private final ArrayList<DtChannel> channelsList;
    private final IntObjMap<DtChannel> channels;
    private final IoQueue ioQueue;

    private final LongObjMap<WriteData> pendingOutgoingRequests = new LongObjMap<>();
    private final CompletableFuture<Void> preCloseFuture = new CompletableFuture<>();

    private final Semaphore requestSemaphore;

    private final SimpleByteBufferPool directPool;
    private final SimpleByteBufferPool heapPool;

    private final WorkerStatus workerStatus;

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

        this.channels = new IntObjMap<>();
        if (config instanceof NioServerConfig) {
            this.channelsList = null;
            this.ioQueue = new IoQueue(null, pendingOutgoingRequests);
        } else {
            this.channelsList = new ArrayList<>();
            this.ioQueue = new IoQueue(channelsList, pendingOutgoingRequests);
        }

        this.directPool = new SimpleByteBufferPool(true, 64,
                config.getBufPoolSize(), config.getBufPoolMinCount(),
                config.getBufPoolMaxCount(), config.getBufPoolTimeout());

        this.heapPool = new SimpleByteBufferPool(false, 64,
                config.getBufPoolSize(), config.getBufPoolMinCount(),
                config.getBufPoolMaxCount(), config.getBufPoolTimeout());

        workerStatus = new WorkerStatus();
        workerStatus.setIoQueue(ioQueue);
        workerStatus.setPendingRequests(pendingOutgoingRequests);
        workerStatus.setWakeupRunnable(this::wakeup);
        workerStatus.setDirectPool(directPool);
        workerStatus.setHeapPool(heapPool);
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
        });
    }

    // invoke by other threads
    public CompletableFuture<Void> connect(Peer peer) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        doInIoThread(() -> doConnect(f, peer));
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
                    cleanTimeoutReq(roundStartTime);
                    directPool.refreshCurrentNanos(roundStartTime);
                    heapPool.refreshCurrentNanos(roundStartTime);
                    directPool.clean(roundStartTime);
                    heapPool.clean(roundStartTime);
                    lastCleanNano = roundStartTime;
                }
            } catch (Throwable e) {
                log.error("", e);
            }
        }
        try {
            selector.close();
            channels.forEach((index, dtc) -> closeChannel0(dtc.getChannel()));
            log.info("worker thread [{}] finished.\n" +
                            "markReadCount={}, markWriteCount={}\n" +
                            "readCount={}, readBytes={}, avgReadBytes={}\n" +
                            "writeCount={}, writeBytes={}, avgWriteBytes={}",
                    workerName, statMarkReadCount, statMarkWriteCount,
                    statReadCount, statReadBytes, statReadCount == 0 ? 0 : statReadBytes / statReadCount,
                    statWriteCount, statWriteBytes, statWriteCount == 0 ? 0 : statWriteBytes / statWriteCount);
            if (log.isDebugEnabled()) {
                log.debug("direct pool stat: {}\nheap pool stat: {}", directPool.formatStat(), heapPool.formatStat());
            }
        } catch (Exception e) {
            log.error("close error. {}", e);
        }
    }

    private long run0(Selector selector, long selectTimeoutMillis, int stopStatus) {
        if (!select(selector, selectTimeoutMillis)) {
            return System.nanoTime();
        }
        long roundTime = System.nanoTime();
        boolean hasDataToWrite = ioQueue.dispatchWriteQueue();
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
            readBuffer = directPool.borrow(config.getReadBufferSize());
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
            directPool.release(readBuffer);
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
                closeChannelBySelKey(key);
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
                statReadCount++;
                prepareReadBuffer(roundTime);
                int readCount = sc.read(readBuffer);
                if (readCount == -1) {
                    // log.info("socket read to end, remove it: {}", key.channel());
                    closeChannelBySelKey(key);
                    return false;
                }
                statReadBytes += readCount;
                readBuffer.flip();
                dtc.afterRead(stopStatus == SS_RUNNING, readBuffer);
            }
            stage = 3;
            if (key.isWritable()) {
                IoSubQueue subQueue = dtc.getSubQueue();
                ByteBuffer buf = subQueue.getWriteBuffer();
                if (buf != null) {
                    hasDataToWrite = true;
                    subQueue.setWriting(true);
                    int x = buf.remaining();
                    sc.write(buf);
                    x = x - buf.remaining();
                    statWriteBytes += x;
                    statWriteCount++;
                } else {
                    // no data to write
                    subQueue.setWriting(false);
                    key.interestOps(SelectionKey.OP_READ);
                    statMarkReadCount++;
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
            closeChannelBySelKey(key);
        }
        return hasDataToWrite;
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
            statMarkWriteCount++;
            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }
    }

    private void doConnect(CompletableFuture<Void> f, Peer peer) {
        PeerStatus s = peer.getStatus();
        if (s != PeerStatus.not_connect) {
            String msg = s == PeerStatus.connected ? "peer connected" : "peer connecting";
            f.completeExceptionally(new NetException(msg));
            return;
        } else {
            peer.setStatus(PeerStatus.connecting);
        }
        SocketChannel sc = null;
        try {
            HostPort hp = peer.getEndPoint();
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
            channels.put(dtc.getChannelIndexInWorker(), dtc);
            channelsList.add(dtc);
            f.complete(null);
        } catch (Exception e) {
            log.warn("connect channel fail: {}, {}", peer.getEndPoint(), e.toString());
            closeChannel0((SocketChannel) key.channel());
            peer.setStatus(PeerStatus.not_connect);
            f.completeExceptionally(new NetException(e));
        }
    }

    public void doInIoThread(Runnable runnable) {
        ioQueue.scheduleFromBizThread(runnable);
        wakeup();
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
        });
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

    private void close(DtChannel dtc) {
        if (!dtc.isClosed()) {
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

    private void cleanTimeoutReq(long roundStartTime) {
        LongObjMap<WriteData> map = this.pendingOutgoingRequests;
        LinkedList<Long> expireList = new LinkedList<>();
        map.forEach((key, d) -> {
            DtTime t = d.getTimeout();
            if (t.rest(TimeUnit.MILLISECONDS, roundStartTime) <= 0) {
                expireList.add(key);
            }
        });
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
        this.ioQueue.writeFromBizThread(data);
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
