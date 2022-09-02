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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;

/**
 * @author huangli
 */
public class NioServer extends NioRemoting implements Runnable {
    private static final DtLog log = DtLogs.getLogger(NioServer.class);

    private final NioServerConfig config;
    private ServerSocketChannel ssc;
    private Selector selector;
    private volatile boolean stop;
    private final Thread acceptThread;
    private final NioWorker[] workers;

    public NioServer(NioServerConfig config) {
        super(config);
        this.config = config;
        if (config.getPort() <= 0) {
            throw new IllegalArgumentException("no port");
        }
        acceptThread = new Thread(this);
        acceptThread.setName(config.getName() + "IoAccept");
        workers = new NioWorker[config.getIoThreads()];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new NioWorker(nioStatus, config.getName() + "IoWorker" + i, config);
        }
    }

    @Override
    public void doStart() throws Exception {
        ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ssc.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        ssc.bind(new InetSocketAddress(config.getPort()));
        selector = SelectorProvider.provider().openSelector();
        ssc.register(selector, SelectionKey.OP_ACCEPT);

        log.info("{} listen at port {}", config.getName(), config.getPort());

        initBizExecutor();
        acceptThread.start();
        for (NioWorker worker : workers) {
            worker.start();
        }
    }

    public void run() {
        while (!stop) {
            select();
        }
        try {
            selector.close();
            ssc.close();
            log.info("accept thread finished: {}", config.getName());
        } catch (Exception e) {
            log.error("close error. name={}, port={}", config.getName(), config.getPort(), e);
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
                    if (log.isDebugEnabled()) {
                        log.debug("Accept SelectionKey is invalid, name={}, port= {}"
                                , config.getName(), config.getPort());
                    }
                    continue;
                }
                if (key.isAcceptable()) {
                    SocketChannel sc = ssc.accept();
                    log.debug("accept new socket: {}", sc);
                    workers[sc.hashCode() % workers.length].newChannelAccept(sc);
                }
            }
        } catch (ClosedSelectorException e) {
            log.warn("selector closed. name={}, port={}", config.getName(), config.getPort());
        } catch (Throwable e) {
            log.error("accept thread failed. name={}, port={}", config.getName(), config.getPort(), e);
        }
    }

    @Override
    public void doStop() throws Exception {
        stop = true;
        if (selector != null) {
            selector.wakeup();
        }
        shutdownBizExecutor();
        for (NioWorker worker : workers) {
            worker.stop();
        }
    }

}
