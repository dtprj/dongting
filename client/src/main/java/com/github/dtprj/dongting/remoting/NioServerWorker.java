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
import java.util.concurrent.atomic.AtomicInteger;

public class NioServerWorker implements LifeCircle, Runnable {
    private static final DtLog log = DtLogs.getLogger(NioServerWorker.class);

    private final String workerName;
    private final Thread thread;
    private final RpcPbCallback pbCallback = new RpcPbCallback();
    private volatile boolean stop;
    private volatile Selector selector;

    private final ConcurrentLinkedQueue<SocketChannel> newChannels = new ConcurrentLinkedQueue<>();
    private final AtomicInteger newChannelsCount = new AtomicInteger();

    private final HashSet<SocketChannel> channels = new HashSet<>();

    public NioServerWorker(NioServerConfig config, int index) {
        this.thread = new Thread(this);
        this.workerName = config.getName() + "IoWorker" + index;
        this.thread.setName(workerName);
    }

    public void add(SocketChannel sc) {
        newChannels.add(sc);
        newChannelsCount.getAndIncrement();
        if (selector != null) {
            selector.wakeup();
        }
    }

    @Override
    public void run() {
        while (!stop) {
            initNewChannels();
            if (!stop) {
                select();
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

    private void initNewChannels() {
        try {
            while (newChannelsCount.get() > 0) {
                SocketChannel sc = newChannels.remove();
                initNewChannel(sc);
                newChannelsCount.decrementAndGet();
            }
        } catch (Throwable e) {
            log.error("init channel failed: {}", workerName, e);
        }
    }

    private void select() {
        try {
            selector.select();
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();
                if (!key.isValid()) {
                    log.info("socket may closed, remove it: {}", key.channel());
                    channels.remove(key);
                }
                if (key.isReadable()) {
                    SocketChannel sc = (SocketChannel) key.channel();
                    ChannelOps op = (ChannelOps) key.attachment();
                    ByteBuffer buf = op.getOrCreateReadBuffer();
                    int readCount = sc.read(buf);
                    if (readCount == -1) {
                        log.info("socket closed, remove it: {}", key.channel());
                        channels.remove(sc);
                        sc.close();
                        continue;
                    }
                    op.afterRead();
                }
                if (key.isWritable()) {

                }
            }
        } catch (ClosedSelectorException e) {
            log.warn("selector closed: {}", workerName);
        } catch (Throwable e) {
            log.error("init thread failed: {}", workerName, e);
        }
    }

    private void initNewChannel(SocketChannel sc) throws Exception {
        sc.configureBlocking(false);
        sc.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
        sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
        sc.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, new ChannelOps(pbCallback));
        channels.add(sc);
        log.info("accepted new socket: " + sc);
    }

    @Override
    public void start() throws Exception {
        selector = SelectorProvider.provider().openSelector();
        thread.start();
    }

    @Override
    public void stop() throws Exception {
        stop = true;
        if (selector != null) {
            selector.wakeup();
        }
    }
}
