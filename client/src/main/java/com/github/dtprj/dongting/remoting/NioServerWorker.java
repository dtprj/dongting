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
import java.nio.channels.ClosedSelectorException;
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
            run0();
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
        try {
            select();
            initNewChannels();
            dispatchWriteQueue();
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();
                SocketChannel sc = (SocketChannel) key.channel();
                if (!key.isValid()) {
                    log.info("socket may closed, remove it: {}", key.channel());
                    channels.remove(sc);
                }
                DtChannel dtc = (DtChannel) key.attachment();
                if (key.isReadable()) {
                    ByteBuffer buf = dtc.getOrCreateReadBuffer();
                    int readCount = sc.read(buf);
                    if (readCount == -1) {
                        log.info("socket closed, remove it: {}", key.channel());
                        channels.remove(sc);
                        sc.close();
                        continue;
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
            }
        } catch (ClosedSelectorException e) {
            log.warn("selector closed: {}", workerName);
        } catch (Throwable e) {
            log.error("init thread failed: {}", workerName, e);
        }
    }

    private void select() throws IOException {
        selector.select();
        notified.set(false);
    }

    private void wakeup() {
        if (notified.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    private void initNewChannels() {
        try {
            SocketChannel sc;
            while ((sc = newChannels.poll()) != null) {
                initNewChannel(sc);
            }
        } catch (Throwable e) {
            log.error("init channel failed: {}", workerName, e);
        }
    }

    private void initNewChannel(SocketChannel sc) throws Exception {
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
        log.info("accepted new socket: " + sc);
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
