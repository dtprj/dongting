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

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.DtThreadFactory;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.ThreadUtils;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author huangli
 */
public abstract class NioNet extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(NioNet.class);
    private final NioConfig config;
    private final Semaphore semaphore;
    protected final NioStatus nioStatus = new NioStatus();
    protected volatile ExecutorService bizExecutor;

    public NioNet(NioConfig config) {
        this.config = config;
        this.semaphore = new Semaphore(config.getMaxOutRequests());
        nioStatus.setRequestSemaphore(semaphore);
    }

    /**
     * register processor use default executor.
     */
    public void register(int cmd, ReqProcessor processor) {
        if (status != LifeStatus.not_start) {
            throw new DtException("processor should register before start");
        }
        processor.setUseDefaultExecutor(true);
        nioStatus.registerProcessor(cmd, processor);
    }

    /**
     * register processor use specific executor, if executorService is null, run in io thread.
     */
    public void register(int cmd, ReqProcessor processor, ExecutorService executorService) {
        if (status != LifeStatus.not_start) {
            throw new DtException("processor should register before start");
        }
        if (executorService == null && !processor.getDecoder().decodeInIoThread()) {
            throwThreadNotMatch(cmd);
        }
        processor.setExecutor(executorService);
        nioStatus.registerProcessor(cmd, processor);
    }

    private void throwThreadNotMatch(int cmd) {
        throw new DtException("the processor should run in io thread," +
                " but decoder.decodeInIoThread==false. command=" + cmd);
    }

    protected CompletableFuture<ReadFrame> sendRequest(NioWorker worker, Peer peer, WriteFrame request,
                                                       Decoder decoder, DtTime timeout) {
        boolean acquire = false;
        try {
            if (status != LifeStatus.running) {
                return errorFuture(new IllegalStateException("error state: " + status));
            }

            acquire = this.semaphore.tryAcquire(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            if (acquire) {
                CompletableFuture<ReadFrame> future = new CompletableFuture<>();
                worker.writeReqInBizThreads(peer, request, decoder, timeout, future);
                return future.thenApply(new RespConvertor(decoder));
            } else {
                return errorFuture(new NetTimeoutException(
                        "too many pending requests, client wait permit timeout in "
                                + timeout.getTimeout(TimeUnit.MILLISECONDS) + " ms"));
            }
        } catch (Throwable e) {
            if (acquire) {
                this.semaphore.release();
            }
            return errorFuture(new NetException("sendRequest error", e));
        }
    }

    private static class RespConvertor implements Function<ReadFrame, ReadFrame> {
        private final Decoder decoder;

        RespConvertor(Decoder decoder) {
            this.decoder = decoder;
        }

        @Override
        public ReadFrame apply(ReadFrame frame) {
            if (frame.getRespCode() != CmdCodes.SUCCESS) {
                throw new NetCodeException(frame.getRespCode());
            }
            if (!decoder.decodeInIoThread()) {
                ByteBuffer buf = (ByteBuffer) frame.getBody();
                if (buf != null) {
                    Object body = decoder.decode(null, buf, buf.remaining(), true, true);
                    frame.setBody(body);
                }
            }
            return frame;
        }
    }

    protected <T> CompletableFuture<T> errorFuture(Throwable e) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(e);
        return f;
    }

    protected void initBizExecutor() {
        if (config.getBizThreads() > 0) {
            bizExecutor = new ThreadPoolExecutor(config.getBizThreads(), config.getBizThreads(),
                    1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(config.getMaxInRequests()),
                    new DtThreadFactory(config.getName() + "Biz", false));
        }
        // so register method can be invoked before or after start
        Iterator<IntObjectCursor<ReqProcessor>> it = nioStatus.getProcessors().iterator();
        while (it.hasNext()) {
            IntObjectCursor<ReqProcessor> en = it.next();
            ReqProcessor p = en.value;
            if (p.isUseDefaultExecutor()) {
                if (bizExecutor != null) {
                    p.setExecutor(bizExecutor);
                } else {
                    if (!p.getDecoder().decodeInIoThread()) {
                        throwThreadNotMatch(en.key);
                    }
                }
            }
        }
    }

    protected void forceStopWorker(NioWorker worker) {
        try {
            worker.stop();
        } catch (Exception e) {
            log.error("force stop worker fail: worker={}, error={}", worker.getThread().getName(), e.toString());
        }
    }

    protected void shutdownBizExecutor(DtTime timeout) {
        if (bizExecutor != null) {
            bizExecutor.shutdown();
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            long rest = timeout.rest(TimeUnit.MILLISECONDS);
            if (rest > 0) {
                try {
                    if (!bizExecutor.awaitTermination(rest, TimeUnit.MILLISECONDS)) {
                        log.warn("bizExecutor not terminated in {} ms", rest);
                    }
                } catch (InterruptedException e) {
                    ThreadUtils.restoreInterruptStatus();
                }
            }
        }
    }
}
