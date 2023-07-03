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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
public abstract class NioNet extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(NioNet.class);
    private final NioConfig config;
    final Semaphore semaphore;
    final NioStatus nioStatus;
    protected volatile ExecutorService bizExecutor;

    public NioNet(NioConfig config) {
        this.config = config;
        this.nioStatus = new NioStatus(config.getMaxInBytes() > 0 ? new AtomicLong(0) : null);
        this.semaphore = config.getMaxOutRequests() > 0 ? new Semaphore(config.getMaxOutRequests()) : null;
        if (config.getMaxFrameSize() < config.getMaxBodySize() + 128 * 1024) {
            throw new IllegalArgumentException("maxFrameSize should greater than maxBodySize plus 128KB.");
        }
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
    public void register(int cmd, ReqProcessor processor, Executor executor) {
        if (status != LifeStatus.not_start) {
            throw new DtException("processor should register before start");
        }
        processor.setExecutor(executor);
        nioStatus.registerProcessor(cmd, processor);
    }

    CompletableFuture<ReadFrame<?>> sendRequest(NioWorker worker, Peer peer, WriteFrame request,
                                                Decoder<?> decoder, DtTime timeout) {
        request.setFrameType(FrameType.TYPE_REQ);
        DtUtil.checkPositive(request.getCommand(), "request.command");
        boolean acquire = false;
        boolean write = false;
        try {
            if (status != LifeStatus.running) {
                return errorFuture(new NetException("error state: " + status));
            }

            if (this.semaphore != null) {
                acquire = this.semaphore.tryAcquire(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                if (!acquire) {
                    return errorFuture(new NetTimeoutException(
                            "too many pending requests, client wait permit timeout in "
                                    + timeout.getTimeout(TimeUnit.MILLISECONDS) + " ms"));
                }
            }

            CompletableFuture<ReadFrame<?>> future = new CompletableFuture<>();
            worker.writeReqInBizThreads(peer, request, decoder, timeout, future);
            write = true;
            return registerReqCallback(future);

        } catch (Exception e) {
            request.clean();
            return errorFuture(new NetException("sendRequest error", e));
        } finally {
            if (acquire && !write) {
                this.semaphore.release();
            }
        }
    }

    private CompletableFuture<ReadFrame<?>> registerReqCallback(CompletableFuture<ReadFrame<?>> future) {
        return future.whenComplete((frame, ex) -> {
            if (semaphore != null) {
                semaphore.release();
            }
            if (ex != null) {
                return;
            }
            if (frame.getRespCode() != CmdCodes.SUCCESS) {
                throw new NetCodeException(frame.getRespCode(), frame.getMsg());
            }
        });
    }

    protected <T> CompletableFuture<T> errorFuture(Throwable e) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(e);
        return f;
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

    void forceStopWorker(NioWorker worker) {
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
                    DtUtil.restoreInterruptStatus();
                }
            }
        }
    }

    public static List<HostPort> parseServers(String serversList) {
        String[] servers = serversList.split(";");
        if (servers.length == 0) {
            throw new IllegalArgumentException("servers list is empty");
        }
        try {
            List<HostPort> list = new ArrayList<>();
            for (String hostPortStr : servers) {
                list.add(parseHostPort(hostPortStr));
            }
            return list;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad servers list: " + serversList);
        }
    }

    public static HostPort parseHostPort(String hostPortStr) {
        int x = hostPortStr.lastIndexOf(':');
        if (x < 0 || x == hostPortStr.length() - 1) {
            throw new IllegalArgumentException("not 'id,host:port' format:" + hostPortStr);
        }
        String host = hostPortStr.substring(0, x).trim();
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        int port = Integer.parseInt(hostPortStr.substring(x + 1).trim());
        return new HostPort(host, port);
    }
}
