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

import com.github.dtprj.dongting.common.LifeCircle;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * each worker represent a thread.
 */
public class NioServerWorker implements LifeCircle, Runnable {
    private static final DtLog log = DtLogs.getLogger(NioServerWorker.class);

    private final String workerName;
    private final Thread thread;
    private final RpcPbCallback pbCallback = new RpcPbCallback();
    private final IoQueue ioQueue = new IoQueue();
    private final NioServerStatus nioServerStatus;
    private volatile boolean stop;
    private Selector selector;
    private volatile AtomicBoolean notified = new AtomicBoolean(false);

    private final ConcurrentLinkedQueue<SocketChannel> newChannels = new ConcurrentLinkedQueue<>();

    private final HashSet<SocketChannel> channels = new HashSet<>();

    public NioServerWorker(NioServerConfig config, int index, NioServerStatus nioServerStatus) {
        this.nioServerStatus = nioServerStatus;
        this.thread = new Thread(this);
        this.workerName = config.getName() + "IoWorker" + index;
        this.thread.setName(workerName);
    }

    public void add(SocketChannel sc) {
        newChannels.add(sc);
        wakeup();
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
            for (SocketChannel sc : channels) {
                sc.close();
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
        initNewChannels();
        dispatchWriteQueue();
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
                closeChannel(sc, key);
                return;
            }
            DtChannel dtc = (DtChannel) key.attachment();
            if (key.isReadable()) {
                ByteBuffer buf = dtc.getOrCreateReadBuffer();
                int readCount = sc.read(buf);
                if (readCount == -1) {
                    log.info("socket closed, remove it: {}", key.channel());
                    closeChannel(sc, key);
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
        } catch (IOException e) {
            log.warn("socket error: {}", e.getMessage());
            closeChannel(sc, key);
        }
    }

    private void closeChannel(SocketChannel sc, SelectionKey key) {
        try {
            channels.remove(sc);
            key.cancel();
            if (sc.isOpen()) {
                sc.close();
            }
        } catch (Exception e) {
            log.warn("close channel fail: {}, {}", sc, e.getMessage());
        }
    }

    private boolean select() {
        try {
            selector.select();
            return true;
        } catch (Exception e) {
            log.error("select failed: {}", workerName, e);
            return false;
        } finally {
            notified.set(false);
        }
    }

    private void wakeup() {
        if (notified.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    private void initNewChannels() {
        SocketChannel sc;
        while ((sc = newChannels.poll()) != null) {
            initNewChannel(sc);
        }
    }

    private void initNewChannel(SocketChannel sc) {
        try {
            sc.configureBlocking(false);
            sc.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
            sc.setOption(StandardSocketOptions.TCP_NODELAY, true);

            WorkerParams workerParams = new WorkerParams();
            workerParams.setChannel(sc);
            workerParams.setCallback(pbCallback);
            workerParams.setIoQueue(ioQueue);
            workerParams.setWakeupRunnable(this::wakeup);
            DtChannel dtc = new DtChannel(nioServerStatus, workerParams);
            SelectionKey selectionKey = sc.register(selector, SelectionKey.OP_READ, dtc);
            dtc.setSelectionKey(selectionKey);

            channels.add(sc);
            log.info("accepted new socket: {}", sc);
        } catch (ClosedChannelException e) {
            log.info("channel is closed: {}", sc);
        } catch (Exception e) {
            log.warn("init channel fail: {}", sc, e);
        }
    }

    private void dispatchWriteQueue() {
        WriteObj data;
        while ((data = ioQueue.poll()) != null) {
            data.getDtc().enqueue(data.getBuffer());
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
}
