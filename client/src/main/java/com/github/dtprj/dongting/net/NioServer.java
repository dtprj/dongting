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
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.RefBufferDecoder;
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
@SuppressWarnings("Convert2Diamond")
public class NioServer extends NioNet implements Runnable {
    private static final DtLog log = DtLogs.getLogger(NioServer.class);

    private final NioServerConfig config;
    private ServerSocketChannel ssc;
    private Selector selector;
    private volatile boolean stop;
    private final Thread acceptThread;
    final NioWorker[] workers;

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
            workers[i] = new NioWorker(nioStatus, config.getName() + "IoWorker" + i, config, null);
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
    public void doStop(DtTime timeout, boolean force) {
        if (force) {
            forceStop(timeout);
            return;
        }
        stopAcceptThread();
        ArrayList<CompletableFuture<Void>> prepareFutures = new ArrayList<>();
        for (NioWorker worker : workers) {
            prepareFutures.add(worker.prepareStop());
        }
        CompletableFuture<Void> f = CompletableFuture.allOf(prepareFutures.toArray(new CompletableFuture[0]));
        try {
            long rest = timeout.rest(TimeUnit.MILLISECONDS);
            f.get(rest, TimeUnit.MILLISECONDS);
            log.info("server {} pre-stop done", config.getName());
        } catch (Exception e) {
            Throwable root = DtUtil.rootCause(e);
            if (root instanceof InterruptedException) {
                log.warn("nio server pre-stop interrupted");
                DtUtil.restoreInterruptStatus();
            } else if (root instanceof TimeoutException) {
                log.warn("server {} pre-stop timeout. {}ms", config.getName(), timeout.getTimeout(TimeUnit.MILLISECONDS));
            } else {
                log.error("server {} pre-stop error", config.getName(), e);
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

        log.info("server {} stopped", config.getName());
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

    public <T> CompletableFuture<ReadFrame<T>> sendRequest(DtChannel dtc, WriteFrame request, Decoder<T> decoder,
                                                           DtTime timeout) {
        CompletableFuture<ReadFrame<T>> f = new CompletableFuture<>();
        push((DtChannelImpl) dtc, request, decoder, timeout, new RpcCallback<T>() {
            @Override
            public void success(ReadFrame<T> resp) {
                f.complete(resp);
            }

            @Override
            public void fail(Throwable ex) {
                f.completeExceptionally(ex);
            }
        });
        return f;
    }

    public <T> void sendRequest(DtChannel dtc, WriteFrame request, Decoder<T> decoder, DtTime timeout,
                                RpcCallback<T> callback) {
        push((DtChannelImpl) dtc, request, decoder, timeout, callback);
    }

    public CompletableFuture<Void> sendOneWay(DtChannel dtc, WriteFrame request, DtTime timeout) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        push((DtChannelImpl) dtc, request, null, timeout, new RpcCallback<Object>() {
            @Override
            public void success(ReadFrame<Object> resp) {
                f.complete(null);
            }

            @Override
            public void fail(Throwable ex) {
                f.completeExceptionally(ex);
            }
        });
        return f;
    }

    public <T> void sendOneWay(DtChannel dtc, WriteFrame request, DtTime timeout, RpcCallback<T> callback) {
        push((DtChannelImpl) dtc, request, null, timeout, callback);
    }

    public NioServerConfig getConfig() {
        return config;
    }

    public static class PingProcessor extends ReqProcessor<RefBuffer> {

        private static final RefBufferDecoder DECODER = RefBufferDecoder.PLAIN_INSTANCE;

        public PingProcessor() {
        }

        @Override
        public WriteFrame process(ReadFrame<RefBuffer> frame, ChannelContext channelContext, ReqContext reqContext) {
            RefBufWriteFrame resp = new RefBufWriteFrame(frame.getBody());
            resp.setRespCode(CmdCodes.SUCCESS);
            return resp;
        }

        @Override
        public Decoder<RefBuffer> createDecoder(int command) {
            return DECODER;
        }

    }
}
