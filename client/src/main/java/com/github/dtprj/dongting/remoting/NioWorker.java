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
package com.github.dtprj.dongting.remoting;

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
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
    private final RpcPbCallback pbCallback = new RpcPbCallback();
    private final IoQueue ioQueue = new IoQueue();
    private final NioStatus nioStatus;
    private final NioConfig config;
    private volatile int stopStatus = SS_RUNNING;
    private Selector selector;
    private final AtomicBoolean notified = new AtomicBoolean(false);

    private final ConcurrentLinkedQueue<Runnable> actions = new ConcurrentLinkedQueue<>();

    private final CopyOnWriteArrayList<DtChannel> channels = new CopyOnWriteArrayList<>();
    private final HashMap<Integer, WriteRequest> pendingOutgoingRequests = new HashMap<>();
    private final CompletableFuture<Void> preCloseFuture = new CompletableFuture<>();

    private final Semaphore requestSemaphore;

    private final ByteBufferPool pool = new ByteBufferPool(true);

    public NioWorker(NioStatus nioStatus, String workerName, NioConfig config) {
        this.nioStatus = nioStatus;
        this.config = config;
        this.thread = new Thread(this);
        this.workerName = workerName;
        this.thread.setName(workerName);
        this.requestSemaphore = nioStatus.getRequestSemaphore();
    }

    // invoke by NioServer accept thead
    public void newChannelAccept(SocketChannel sc) {
        actions.add(() -> {
            try {
                initNewChannel(sc);
            } catch (Exception e) {
                log.warn("accept channel fail: {}, {}", sc, e.toString());
                closeChannel(sc, null);
            }
        });
        wakeup();
    }

    // invoke by other threads
    public CompletableFuture<DtChannel> connect(SocketAddress socketAddress) {
        CompletableFuture<DtChannel> f = new CompletableFuture<>();
        actions.add(() -> doConnect(socketAddress, f));
        wakeup();
        return f;
    }

    // invoke by other threads
    public CompletableFuture<Void> close(SocketChannel channel) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        actions.add(() -> closeChannel(channel, f));
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
            for (DtChannel c : channels) {
                c.getChannel().close();
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
        Object att = key.attachment();
        if (att instanceof DtChannel) {
            closeChannel((DtChannel) att, null);
        } else {
            closeChannel((SocketChannel) att, null);
        }
    }

    private void closeChannel(SocketChannel sc, CompletableFuture<Void> f) {
        channels.removeIf(dtc -> dtc.getChannel() == sc);
        closeChannel0(sc, f);
    }

    private void closeChannel(DtChannel dtc, CompletableFuture<Void> f) {
        channels.remove(dtc);
        closeChannel0(dtc.getChannel(), f);
    }

    private void closeChannel0(SocketChannel sc, CompletableFuture<Void> f) {
        try {
            if (sc.isOpen()) {
                log.info("closing channel: {}", sc);
                sc.close();
            }
            if (f != null) {
                f.complete(null);
            }
        } catch (Exception e) {
            log.warn("close channel fail: {}, {}", sc, e.getMessage());
            if (f != null) {
                f.completeExceptionally(new RemotingException(e));
            }
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

    private DtChannel initNewChannel(SocketChannel sc) throws IOException {
        sc.configureBlocking(false);
        sc.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
        sc.setOption(StandardSocketOptions.TCP_NODELAY, true);

        WorkerParams workerParams = new WorkerParams();
        workerParams.setChannel(sc);
        workerParams.setCallback(pbCallback);
        workerParams.setIoQueue(ioQueue);
        workerParams.setPendingRequests(pendingOutgoingRequests);
        workerParams.setWakeupRunnable(this::wakeup);
        workerParams.setPool(pool);
        DtChannel dtc = new DtChannel(nioStatus, workerParams);
        SelectionKey selectionKey = sc.register(selector, SelectionKey.OP_READ, dtc);
        dtc.setSelectionKey(selectionKey);

        channels.add(dtc);
        log.info("new DtChannel init: {}", sc);
        return dtc;
    }

    private void doConnect(SocketAddress socketAddress, CompletableFuture<DtChannel> f) {
        SocketChannel sc = null;
        try {
            sc = SocketChannel.open();
            sc.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
            sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
            sc.configureBlocking(false);
            sc.register(selector, SelectionKey.OP_CONNECT, f);
            sc.connect(socketAddress);
        } catch (Throwable e) {
            if (sc != null) {
                closeChannel(sc, null);
            }
            f.completeExceptionally(new RemotingException(e));
        }
    }

    private void processConnect(SelectionKey key) {
        CompletableFuture<DtChannel> f = (CompletableFuture<DtChannel>) key.attachment();
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            channel.finishConnect();
            DtChannel dtc = initNewChannel(channel);
            f.complete(dtc);
        } catch (Exception e) {
            log.warn("init channel fail: {}, {}", channel, e.toString());
            closeChannel(key);
            f.completeExceptionally(new RemotingException(e));
        }
    }

    private void cleanTimeoutReq() {
        Iterator<Map.Entry<Integer, WriteRequest>> it = this.pendingOutgoingRequests.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, WriteRequest> en = it.next();
            DtTime t = en.getValue().getTimeout();
            if (t.rest(TimeUnit.MILLISECONDS) <= 0) {
                it.remove();
                log.debug("drop timeout request: {}ms, seq={}, {}",
                        t.getTimeout(TimeUnit.MILLISECONDS), en.getValue().getData().getSeq(),
                        en.getValue().getDtc().getChannel());
                String msg = "timeout: " + t.getTimeout(TimeUnit.MILLISECONDS) + "ms";
                en.getValue().getFuture().completeExceptionally(new RemotingTimeoutException(msg));
                requestSemaphore.release();
            }
        }
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

    public List<DtChannel> getChannels() {
        return channels;
    }

    public Thread getThread() {
        return thread;
    }

    public CompletableFuture<Void> getPreCloseFuture() {
        return preCloseFuture;
    }
}
