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

import com.github.dtprj.dongting.buf.RefCountByteBuffer;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.ThreadUtils;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author huangli
 */
public class NioServer extends NioNet implements Runnable {
    private static final DtLog log = DtLogs.getLogger(NioServer.class);

    private final NioServerConfig config;
    private ServerSocketChannel ssc;
    private Selector selector;
    private volatile boolean stop;
    private final Thread acceptThread;
    private final NioWorker[] workers;

    private static final PingProcessor PING_PROCESSOR = new PingProcessor();

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
        register(Commands.CMD_PING, PING_PROCESSOR);
    }

    @Override
    public void doStart() {
        try {
            ssc = ServerSocketChannel.open();
            ssc.configureBlocking(false);
            ssc.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            ssc.bind(new InetSocketAddress(config.getPort()), config.getBacklog());
            selector = SelectorProvider.provider().openSelector();
            ssc.register(selector, SelectionKey.OP_ACCEPT);

            log.info("{} listen at port {}", config.getName(), config.getPort());

            initBizExecutor();
            acceptThread.start();
            for (NioWorker worker : workers) {
                worker.start();
            }
        } catch (IOException e) {
            throw new NetException(e);
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
    public void doStop() {
        DtTime timeout = new DtTime(config.getCloseTimeoutMillis(), TimeUnit.MILLISECONDS);
        stopAcceptThread();
        if (selector != null) {
            selector.wakeup();
        }
        for (NioWorker worker : workers) {
            worker.preStop();
        }
        for (NioWorker worker : workers) {
            long rest = timeout.rest(TimeUnit.MILLISECONDS);
            if (rest > 0) {
                try {
                    worker.getPreCloseFuture().get(rest, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    ThreadUtils.restoreInterruptStatus();
                } catch (TimeoutException e) {
                    break;
                } catch (ExecutionException e) {
                    BugLog.log(e);
                }
            }
        }
        for (NioWorker worker : workers) {
            worker.stop();
        }
        for (NioWorker worker : workers) {
            long rest = timeout.rest(TimeUnit.MILLISECONDS);
            if (rest > 0) {
                try {
                    worker.getThread().join(rest);
                } catch (InterruptedException e) {
                    ThreadUtils.restoreInterruptStatus();
                }
            }
        }
        shutdownBizExecutor(timeout);
    }

    private void stopAcceptThread() {
        stop = true;
        try {
            acceptThread.join(100);
        } catch (InterruptedException e) {
            ThreadUtils.restoreInterruptStatus();
        }
    }

    @Override
    protected void forceStop() {
        log.warn("force stop begin");
        if (acceptThread.isAlive()) {
            stopAcceptThread();
            for (NioWorker worker : workers) {
                forceStopWorker(worker);
            }
        } else {
            if (ssc != null && ssc.isOpen()) {
                try {
                    if (selector != null) {
                        selector.close();
                    }
                    ssc.close();
                } catch (IOException e) {
                    log.error("", e);
                }
            }
        }
        shutdownBizExecutor(new DtTime());
        log.warn("force stop done");
    }

    public static class PingProcessor extends ReqProcessor {

        private static final ByteBufferDecoder DECODER = new ByteBufferDecoder(1024);

        public PingProcessor() {
        }

        @Override
        public WriteFrame process(ReadFrame frame, ProcessContext context) {
            RefCountBufWriteFrame resp = new RefCountBufWriteFrame((RefCountByteBuffer) frame.getBody());
            resp.setRespCode(CmdCodes.SUCCESS);
            return resp;
        }

        @Override
        public Decoder getDecoder() {
            return DECODER;
        }

    }
}
