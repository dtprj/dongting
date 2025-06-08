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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.codec.RefBufferDecoderCallback;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
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
    final NioWorker[] workers;

    public NioServer(NioServerConfig config) {
        super(config);
        this.config = config;
        if (config.port <= 0) {
            throw new IllegalArgumentException("no port");
        }
        acceptThread = new Thread(this);
        acceptThread.setName(config.name + "IoAccept");
        workers = new NioWorker[config.ioThreads];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new NioWorker(nioStatus, config.name + "IoWorker" + i, config, this);
        }
        register(Commands.CMD_PING, new PingProcessor());
        register(Commands.CMD_HANDSHAKE, new HandshakeProcessor(config), null);
    }

    @Override
    public void doStart() {
        try {
            ssc = ServerSocketChannel.open();
            ssc.configureBlocking(false);
            ssc.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            ssc.bind(new InetSocketAddress(config.port), config.backlog);
            selector = SelectorProvider.provider().openSelector();
            ssc.register(selector, SelectionKey.OP_ACCEPT);

            log.info("{} listen at port {}", config.name, config.port);

            createBizExecutor();
            for (NioWorker worker : workers) {
                worker.start();
            }
            acceptThread.start();
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
            log.info("accept thread finished: {}", config.name);
        } catch (Exception e) {
            log.error("close error. name={}, port={}", config.name, config.port, e);
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
                                , config.name, config.port);
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
            log.warn("selector closed. name={}, port={}", config.name, config.port);
        } catch (Throwable e) {
            log.error("accept thread failed. name={}, port={}", config.name, config.port, e);
        }
    }

    @Override
    public void doStop(DtTime timeout, boolean force) {
        if (force) {
            forceStop(timeout);
            return;
        }
        stopAcceptThread();
        ArrayList<CompletableFuture<Void>> prepareFutures = new ArrayList<>();
        for (NioWorker worker : workers) {
            prepareFutures.add(worker.prepareStop(timeout, true));
        }
        CompletableFuture<Void> f = CompletableFuture.allOf(prepareFutures.toArray(new CompletableFuture[0]));
        try {
            long rest = timeout.rest(TimeUnit.MILLISECONDS);
            f.get(rest, TimeUnit.MILLISECONDS);
            log.info("server {} pre-stop done", config.name);
        } catch (Exception e) {
            Throwable root = DtUtil.rootCause(e);
            if (root instanceof InterruptedException) {
                log.warn("nio server pre-stop interrupted");
                DtUtil.restoreInterruptStatus();
            } else if (root instanceof TimeoutException) {
                log.warn("server {} pre-stop timeout. {}ms", config.name, timeout.getTimeout(TimeUnit.MILLISECONDS));
            } else {
                log.error("server {} pre-stop error", config.name, e);
            }
        }

        for (NioWorker worker : workers) {
            stopWorker(worker, timeout);
        }
        for (NioWorker worker : workers) {
            long rest = timeout.rest(TimeUnit.MILLISECONDS);
            if (rest > 0) {
                try {
                    worker.getThread().join(rest);
                } catch (InterruptedException e) {
                    DtUtil.restoreInterruptStatus();
                    break;
                }
            }
        }
        shutdownBizExecutor(timeout);

        log.info("server {} stopped", config.name);
    }

    private void stopAcceptThread() {
        stop = true;
        if (selector != null) {
            selector.wakeup();
        }
        try {
            acceptThread.join(100);
        } catch (InterruptedException e) {
            DtUtil.restoreInterruptStatus();
        }
    }

    private void forceStop(DtTime timeout) {
        log.warn("force stop begin");
        if (acceptThread.isAlive()) {
            stopAcceptThread();
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
        for (NioWorker worker : workers) {
            stopWorker(worker, timeout);
        }
        shutdownBizExecutor(new DtTime());
        log.warn("force stop done");
    }

    public <T> void sendRequest(DtChannel dtc, WritePacket request, DecoderCallbackCreator<T> decoder,
                                DtTime timeout, RpcCallback<T> callback) {
        push((DtChannelImpl) dtc, request, decoder, timeout, callback);
    }

    public <T> void sendOneWay(DtChannel dtc, WritePacket request, DtTime timeout, RpcCallback<T> callback) {
        push((DtChannelImpl) dtc, request, null, timeout, callback);
    }

    public NioServerConfig getConfig() {
        return config;
    }

    public static class PingProcessor extends ReqProcessor<RefBuffer> {

        public PingProcessor() {
        }

        @Override
        public WritePacket process(ReadPacket<RefBuffer> packet, ReqContext reqContext) {
            RefBufWritePacket resp = new RefBufWritePacket(packet.getBody());
            resp.setRespCode(CmdCodes.SUCCESS);
            return resp;
        }

        @Override
        public DecoderCallback<RefBuffer> createDecoderCallback(int command, DecodeContext c) {
            return new RefBufferDecoderCallback(true);
        }

    }
}
