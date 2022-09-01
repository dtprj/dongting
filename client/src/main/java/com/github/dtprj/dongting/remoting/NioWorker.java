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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.LifeCircle;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * each worker represent a thread.
 *
 * @author huangli
 */
public class NioWorker implements LifeCircle, Runnable {
    private static final DtLog log = DtLogs.getLogger(NioWorker.class);

    private final String workerName;
    private final Thread thread;
    private final RpcPbCallback pbCallback = new RpcPbCallback();
    private final IoQueue ioQueue = new IoQueue();
    private final NioStatus nioStatus;
    private volatile boolean stop;
    private Selector selector;
    private final AtomicBoolean notified = new AtomicBoolean(false);

    private final ConcurrentLinkedQueue<Runnable> actions = new ConcurrentLinkedQueue<>();

    private final CopyOnWriteArrayList<DtChannel> channels = new CopyOnWriteArrayList<>();
    private final HashMap<Integer, WriteObj> pendingRequests = new HashMap<>();
    private int selectTimeout;

    public NioWorker(NioStatus nioStatus, String workerName) {
        this.nioStatus = nioStatus;
        this.thread = new Thread(this);
        this.workerName = workerName;
        this.thread.setName(workerName);
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
        while (!stop) {
            try {
                run0();
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

    private void run0() {
        if (!select()) {
            return;
        }
        performActions();
        dispatchWriteQueue();
        dropTimeoutReq();
        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
        while (iterator.hasNext()) {
            processOneSelectionKey(iterator);
        }
    }

    private void processOneSelectionKey(Iterator<SelectionKey> iterator) {
        SelectionKey key = iterator.next();
        iterator.remove();
        SocketChannel sc = (SocketChannel) key.channel();
        try {
            if (!key.isValid()) {
                log.info("socket may closed, remove it: {}", key.channel());
                closeChannel(key);
                return;
            }
            if (key.isConnectable()) {
                processConnect(key);
                return;
            }

            DtChannel dtc = (DtChannel) key.attachment();
            if (key.isReadable()) {
                ByteBuffer buf = dtc.getOrCreateReadBuffer();
                int readCount = sc.read(buf);
                if (readCount == -1) {
                    log.info("socket closed, remove it: {}", key.channel());
                    closeChannel(key);
                    return;
                }
                dtc.afterRead();
            }
            if (key.isWritable()) {
                ByteBuffer buf = dtc.getWriteBuffer();
                if (buf != null) {
                    sc.write(buf);
                } else {
                    // no data to write
                    key.interestOps(SelectionKey.OP_READ);
                }
            }
        } catch (Exception e) {
            log.warn("socket error: {}", e.getMessage());
            closeChannel(key);
        }
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
                sc.close();
                log.info("channel closed: {}", sc);
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

    private boolean select() {
        try {
            if (selectTimeout > 0) {
                selector.select(selectTimeout);
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
        workerParams.setPendingRequests(pendingRequests);
        workerParams.setWakeupRunnable(this::wakeup);
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

    private void dispatchWriteQueue() {
        WriteObj wo;
        while ((wo = ioQueue.poll()) != null) {
            Frame req = wo.getData();
            if (req.getFrameType() == CmdType.TYPE_REQ) {
                WriteObj old = pendingRequests.put(req.getSeq(), wo);
                if (old != null) {
                    String errMsg = "dup seq: old=" + old.getData() + ", new=" + req;
                    log.error(errMsg);
                    wo.getFuture().completeExceptionally(new RemotingException(errMsg));
                    pendingRequests.put(req.getSeq(), old);
                    continue;
                }
            }
            wo.getDtc().enqueue(req.toByteBuffer());
        }
    }

    private void dropTimeoutReq() {
        Iterator<Map.Entry<Integer, WriteObj>> it = pendingRequests.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, WriteObj> en = it.next();
            DtTime t = en.getValue().getTimeout();
            if (t.rest(TimeUnit.MILLISECONDS) <= 0) {
                it.remove();
                String msg = "timeout: " + t.getTimeout(TimeUnit.MILLISECONDS) + "ms";
                en.getValue().getFuture().completeExceptionally(new RemotingTimeoutException(msg));
            }
        }
    }

    @Override
    public synchronized void start() throws Exception {
        selector = SelectorProvider.provider().openSelector();
        thread.start();
    }

    @Override
    public synchronized void stop() throws Exception {
        stop = true;
        wakeup();
    }

    public List<DtChannel> getChannels() {
        return channels;
    }

    public void setSelectTimeout(int selectTimeout) {
        this.selectTimeout = selectTimeout;
    }
}
