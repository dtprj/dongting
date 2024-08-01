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

import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.DtThreadFactory;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author huangli
 */
@SuppressWarnings("Convert2Diamond")
public abstract class NioNet extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(NioNet.class);
    private final NioConfig config;
    final Semaphore semaphore;
    final NioStatus nioStatus;
    protected volatile ExecutorService bizExecutor;
    private final PerfCallback perfCallback;

    public NioNet(NioConfig config) {
        this.config = config;
        this.nioStatus = new NioStatus(config.getMaxInBytes() > 0 ? new AtomicLong(0) : null);
        this.semaphore = config.getMaxOutRequests() > 0 ? new Semaphore(config.getMaxOutRequests()) : null;
        this.perfCallback = config.getPerfCallback();
        if (config.getMaxFrameSize() < config.getMaxBodySize() + 128 * 1024) {
            throw new IllegalArgumentException("maxFrameSize should greater than maxBodySize plus 128KB.");
        }
    }

    /**
     * register processor use default executor.
     */
    public void register(int cmd, ReqProcessor<?> processor) {
        if (status != STATUS_NOT_START) {
            throw new DtException("processor should register before start");
        }
        processor.setUseDefaultExecutor(true);
        nioStatus.registerProcessor(cmd, processor);
    }

    /**
     * register processor use specific executor, if executorService is null, run in io thread.
     */
    public void register(int cmd, ReqProcessor<?> processor, Executor executor) {
        if (status != STATUS_NOT_START) {
            throw new DtException("processor should register before start");
        }
        processor.setExecutor(executor);
        nioStatus.registerProcessor(cmd, processor);
    }

    <T> void send(NioWorker worker, Peer peer, WriteFrame request, Decoder<T> decoder, DtTime timeout,
                  RpcCallback<T> callback) {
        boolean acquire = false;
        try {
            Objects.requireNonNull(timeout);
            Objects.requireNonNull(callback);
            Objects.requireNonNull(request);
            DtUtil.checkPositive(request.getCommand(), "request.command");

            request.setFrameType(decoder != null ? FrameType.TYPE_REQ : FrameType.TYPE_ONE_WAY);
            if (status != STATUS_RUNNING) {
                throw new NetException("error state: " + status);
            }

            if (this.semaphore != null) {
                long t = perfCallback.takeTime(PerfConsts.RPC_D_ACQUIRE);
                try {
                    acquire = this.semaphore.tryAcquire(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                } finally {
                    perfCallback.fireTime(PerfConsts.RPC_D_ACQUIRE, t);
                }
                if (!acquire) {
                    throw new NetTimeoutException("too many pending requests, client wait permit timeout in "
                                    + timeout.getTimeout(TimeUnit.MILLISECONDS) + " ms");
                }
            }
        } catch (Exception e) {
            RpcCallback.callFail(callback, e);
            request.clean();
            if (acquire) {
                semaphore.release();
            }
            return;
        }

        WriteData wd = new WriteData(peer, request, timeout, wrapCallback(callback, request.frameType), decoder);
        worker.writeReqInBizThreads(wd);
    }

    private <T> RpcCallback<T> wrapCallback(RpcCallback<T> callback, int frameType) {
        return new RpcCallback<T>() {
            @Override
            public void success(ReadFrame<T> resp) {
                if (semaphore != null) {
                    semaphore.release();
                }
                if (frameType == FrameType.TYPE_REQ && resp != null && resp.respCode != CmdCodes.SUCCESS) {
                    callback.fail(new NetCodeException(resp.respCode, resp.msg, resp));
                } else {
                    callback.success(resp);
                }
            }

            @Override
            public void fail(Throwable ex) {
                if (semaphore != null) {
                    semaphore.release();
                }
                callback.fail(ex);
            }
        };
    }

    protected void initBizExecutor() {
        if (config.getBizThreads() > 0) {
            int maxReq = config.getMaxInRequests();
            LinkedBlockingQueue<Runnable> queue = maxReq > 0 ? new LinkedBlockingQueue<>(maxReq) : new LinkedBlockingQueue<>();
            bizExecutor = new ThreadPoolExecutor(config.getBizThreads(), config.getBizThreads(),
                    1, TimeUnit.MINUTES, queue,
                    new DtThreadFactory(config.getName() + "Biz", false));
        }
        nioStatus.getProcessors().forEach((command, p) -> {
            if (p.isUseDefaultExecutor()) {
                if (bizExecutor != null) {
                    p.setExecutor(bizExecutor);
                }
            }
        });
    }

    void stopWorker(NioWorker worker, DtTime timeout) {
        try {
            if (worker != null && worker.getStatus() != STATUS_NOT_START) {
                worker.stop(timeout);
            }
        } catch (Exception e) {
            log.error("stop worker fail: worker={}, error={}", worker.getThread().getName(), e.toString());
        }
    }

    protected void shutdownBizExecutor(DtTime timeout) {
        if (bizExecutor != null) {
            bizExecutor.shutdown();
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            if (timeout != null) {
                long rest = timeout.rest(TimeUnit.MILLISECONDS);
                if (rest > 0) {
                    try {
                        if (!bizExecutor.awaitTermination(rest, TimeUnit.MILLISECONDS)) {
                            log.warn("bizExecutor not terminated in {} ms", rest);
                        }
                    } catch (InterruptedException e) {
                        DtUtil.restoreInterruptStatus();
                    }
                }
            }
        }
    }

    public static HostPort parseHostPort(String hostPortStr) {
        int x = hostPortStr.lastIndexOf(':');
        if (x < 0 || x == hostPortStr.length() - 1) {
            throw new IllegalArgumentException("not 'host:port' format:" + hostPortStr);
        }
        String host = hostPortStr.substring(0, x).trim();
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        int port = Integer.parseInt(hostPortStr.substring(x + 1).trim());
        return new HostPort(host, port);
    }
}
