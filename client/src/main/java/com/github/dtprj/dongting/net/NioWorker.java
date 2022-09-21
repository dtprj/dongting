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

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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

    private final Collection<DtChannel> channels;
    private final IoQueue ioQueue;

    private final HashMap<Integer, WriteData> pendingOutgoingRequests = new HashMap<>();
    private final CompletableFuture<Void> preCloseFuture = new CompletableFuture<>();

    private final Semaphore requestSemaphore;

    private final ByteBufferPool pool = new ByteBufferPool(true);

    private final WorkerParams workerParams;

    public NioWorker(NioStatus nioStatus, String workerName, NioConfig config) {
        this.nioStatus = nioStatus;
        this.config = config;
        this.thread = new Thread(this);
        this.workerName = workerName;
        this.thread.setName(workerName);
        this.requestSemaphore = nioStatus.getRequestSemaphore();

        if (config instanceof NioServerConfig) {
            this.channels = new HashSet<>();
            this.ioQueue = new IoQueue(null);
        } else {
            this.channels = new ArrayList<>();
            this.ioQueue = new IoQueue((ArrayList<DtChannel>) channels);
        }

        RpcPbCallback pbCallback = new RpcPbCallback();
        workerParams = new WorkerParams();
        workerParams.setCallback(pbCallback);
        workerParams.setIoQueue(ioQueue);
        workerParams.setPendingRequests(pendingOutgoingRequests);
        workerParams.setWakeupRunnable(this::wakeup);
        workerParams.setPool(pool);
        workerParams.setWorkerName(workerName);
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
        HostPort hp = (HostPort) peer.getEndPoint();
        InetSocketAddress addr = new InetSocketAddress(hp.getHost(), hp.getPort());
        CompletableFuture<Void> f = new CompletableFuture<>();
        actions.add(() -> doConnect(addr, f, hp));
        wakeup();
        return f;
    }

    @Override
    public void run() {
        long cleanIntervalNanos = (long) config.getCleanIntervalMills() * 1000 * 1000;
        long lastCleanNano = System.nanoTime();
        int selectTimeoutMillis = config.getSelectTimeoutMillis();
        Selector selector = this.selector;
        int stopStatus;
        while ((stopStatus = this.stopStatus) <= SS_PRE_STOP) {
            try {
                long roundStartTime = System.nanoTime();
                run0(selector, selectTimeoutMillis, stopStatus, roundStartTime);
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

    private void run0(Selector selector, int selectTimeoutMillis, int stopStatus, long roundTime) {
        if (!select(selector, selectTimeoutMillis)) {
            return;
        }
        performActions();
        boolean hasDataToWrite = ioQueue.dispatchWriteQueue(pendingOutgoingRequests);
        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
        while (iterator.hasNext()) {
            hasDataToWrite |= processOneSelectionKey(iterator, stopStatus, roundTime);
        }
        if (stopStatus == SS_PRE_STOP) {
            if (!hasDataToWrite && pendingOutgoingRequests.size() == 0) {
                preCloseFuture.complete(null);
            }
        }
    }

    private boolean processOneSelectionKey(Iterator<SelectionKey> iterator, int stopStatus, long roundTime) {
        SelectionKey key = iterator.next();
        iterator.remove();
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
                ByteBuffer buf = dtc.getOrCreateReadBuffer();
                int readCount = sc.read(buf);
                if (readCount == -1) {
                    log.info("socket read to end, remove it: {}", key.channel());
                    closeChannel(key);
                    return false;
                }
                dtc.afterRead(stopStatus == SS_RUNNING);
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
                    log.warn("process socket write error: {}", e.toString(), e);
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
            dtc.close();
            channels.remove(dtc);
        } else {
            channels.removeIf(d -> {
                if (d.getChannel() == sc) {
                    d.close();
                    return true;
                } else {
                    return false;
                }
            });
        }
        closeChannel0(sc);
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

    private boolean select(Selector selector, int selectTimeoutMillis) {
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

    private DtChannel initNewChannel(SocketChannel sc, HostPort hostPort) throws IOException {
        sc.configureBlocking(false);
        sc.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
        sc.setOption(StandardSocketOptions.TCP_NODELAY, true);

        DtChannel dtc = new DtChannel(nioStatus, workerParams, config, sc);
        SelectionKey selectionKey = sc.register(selector, SelectionKey.OP_READ, dtc);
        dtc.getSubQueue().setRegisterForWrite(new RegWriteRunner(selectionKey));

        Peer peer = hostPort == null ? new Peer(sc.getRemoteAddress(), true) : new Peer(hostPort, false);
        peer.setDtChannel(dtc);

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

    private void doConnect(SocketAddress socketAddress, CompletableFuture<Void> f, HostPort hp) {
        SocketChannel sc = null;
        try {
            sc = SocketChannel.open();
            sc.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
            sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
            sc.configureBlocking(false);
            sc.register(selector, SelectionKey.OP_CONNECT, new Object[]{f, hp});
            sc.connect(socketAddress);
        } catch (Throwable e) {
            if (sc != null) {
                closeChannel0(sc);
            }
            f.completeExceptionally(new NetException(e));
        }
    }

    private void processConnect(SelectionKey key) {
        Object[] att = (Object[]) key.attachment();
        CompletableFuture<Void> f = (CompletableFuture<Void>) att[0];
        HostPort hp = (HostPort) att[1];
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            channel.finishConnect();
            DtChannel dtc = initNewChannel(channel, hp);
            channels.add(dtc);
            f.complete(null);
        } catch (Exception e) {
            log.warn("init channel fail: {}, {}", channel, e.toString());
            closeChannel0((SocketChannel) key.channel());
            f.completeExceptionally(new NetException(e));
        }
    }

    private void cleanTimeoutReq() {
        Iterator<Map.Entry<Integer, WriteData>> it = this.pendingOutgoingRequests.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, WriteData> en = it.next();
            DtTime t = en.getValue().getTimeout();
            if (t.rest(TimeUnit.MILLISECONDS) <= 0) {
                it.remove();
                log.debug("drop timeout request: {}ms, seq={}, {}",
                        t.getTimeout(TimeUnit.MILLISECONDS), en.getValue().getData().getSeq(),
                        en.getValue().getDtc());
                String msg = "timeout: " + t.getTimeout(TimeUnit.MILLISECONDS) + "ms";
                en.getValue().getFuture().completeExceptionally(new NetTimeoutException(msg));
                requestSemaphore.release();
            }
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
    public void doStart() throws Exception {
        selector = SelectorProvider.provider().openSelector();
        thread.start();
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

    public String getWorkerName() {
        return workerName;
    }

    public Thread getThread() {
        return thread;
    }

    public CompletableFuture<Void> getPreCloseFuture() {
        return preCloseFuture;
    }
}
