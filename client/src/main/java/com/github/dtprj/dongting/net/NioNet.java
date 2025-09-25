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

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.DtThreadFactory;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.PerfConsts;
import com.github.dtprj.dongting.common.VersionFactory;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public abstract class NioNet extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(NioNet.class);
    private final NioConfig config;
    final NioStatus nioStatus;
    ScheduledExecutorService bizExecutor;
    private final PerfCallback perfCallback;

    protected final ReentrantLock lock = new ReentrantLock();
    private final Condition pendingReqCond = lock.newCondition();
    private final Condition pendingBytesCond = lock.newCondition();
    int pendingRequests;
    int pendingBytes;

    private final boolean server;

    public NioNet(NioConfig config) {
        this.config = config;
        this.nioStatus = new NioStatus();
        this.perfCallback = config.perfCallback;
        this.server = config instanceof NioServerConfig;
        if (config.maxPacketSize < config.maxBodySize + 128 * 1024) {
            throw new IllegalArgumentException("maxPacketSize should greater than maxBodySize plus 128KB.");
        }
    }

    /**
     * register processor use default executor.
     */
    public void register(int cmd, ReqProcessor<?> processor) {
        if (status != STATUS_NOT_START) {
            throw new DtException("processor should register before start");
        }
        processor.useDefaultExecutor = true;
        nioStatus.registerProcessor(cmd, processor);
    }

    /**
     * register processor use specific executor, if executorService is null, run in io thread.
     */
    public void register(int cmd, ReqProcessor<?> processor, Executor executor) {
        if (status != STATUS_NOT_START) {
            throw new DtException("processor should register before start");
        }
        processor.executor = executor;
        nioStatus.registerProcessor(cmd, processor);
    }

    <T> void send(NioWorker worker, Peer peer, WritePacket request, DecoderCallbackCreator<T> decoder,
                  DtTime timeout, RpcCallback<T> callback) {
        send0(worker, peer, null, request, decoder, timeout, callback);
    }

    <T> void push(DtChannelImpl dtc, WritePacket request, DecoderCallbackCreator<T> decoder,
                  DtTime timeout, RpcCallback<T> callback) {
        send0(dtc.workerStatus.worker, null, dtc, request, decoder, timeout, callback);
    }

    private <T> void send0(NioWorker worker, Peer peer, DtChannelImpl dtc, WritePacket request,
                           DecoderCallbackCreator<T> decoder, DtTime timeout,
                           RpcCallback<T> callback) {
        boolean acquirePermit = false;
        try {
            generalCheck(request, timeout, callback);
            request.packetType = decoder != null ? PacketType.TYPE_REQ : PacketType.TYPE_ONE_WAY;

            if (!server && request.command != Commands.CMD_HANDSHAKE && !request.acquirePermit) {
                acquirePermit = acquirePermit(request, timeout);
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                DtUtil.restoreInterruptStatus();
            }
            FutureCallback.callFail(callback, e);
            request.clean();
            return;
        }
        if (acquirePermit) {
            RpcCallback<T> old = callback;
            callback = (result, ex) -> {
                releasePermit(request);
                if (old != null) {
                    old.call(result, ex);
                }
            };
        }
        PacketInfo wd;
        if (peer != null) {
            wd = new PacketInfo(peer, request, timeout, callback, decoder);
        } else {
            wd = new PacketInfo(dtc, request, timeout, callback, decoder);
        }
        worker.writeReqInBizThreads(wd);
    }

    public boolean acquirePermit(WritePacket request, DtTime timeout) throws InterruptedException {
        while (true) {
            int maxPending = config.maxOutRequests;
            long maxPendingBytes = config.maxOutBytes;
            if (maxPending <= 0 && maxPendingBytes <= 0) {
                return false;
            }

            long t = perfCallback.takeTime(PerfConsts.RPC_D_ACQUIRE);
            lock.lock();
            try {
                if (request.acquirePermit) {
                    throw new IllegalStateException("current request is already acquired permit");
                }
                if (maxPending > 0 && pendingRequests + 1 > maxPending) {
                    if (!pendingReqCond.await(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)) {
                        throw new NetTimeoutException("too many pending requests, client wait permit timeout in "
                                + timeout.getTimeout(TimeUnit.MILLISECONDS) + " ms");
                    }
                    // re-check all
                    continue;
                }
                int estimateSize = request.calcMaxPacketSize();
                if (maxPendingBytes > 0 && pendingBytes + estimateSize > maxPendingBytes) {
                    if (!pendingBytesCond.await(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)) {
                        throw new NetTimeoutException("too many pending bytes, client wait permit timeout in "
                                + timeout.getTimeout(TimeUnit.MILLISECONDS) + " ms");
                    }
                    // re-check all
                    continue;
                }
                request.acquirePermit = true;
                pendingRequests++;
                pendingBytes += estimateSize;
            } finally {
                lock.unlock();
                perfCallback.fireTime(PerfConsts.RPC_D_ACQUIRE, t);
            }
            return true;
        }
    }

    private <T> void generalCheck(WritePacket request, DtTime timeout, RpcCallback<T> callback) {
        Objects.requireNonNull(timeout);
        Objects.requireNonNull(callback);
        Objects.requireNonNull(request);
        DtUtil.checkPositive(request.command, "request.command");

        if (status != STATUS_RUNNING) {
            throw new NetException("error state: " + status);
        }

        // can't invoke actualSize() here because seq and timeout field is not set yet
        int estimateSize = request.calcMaxPacketSize();
        if (estimateSize > config.maxPacketSize || estimateSize < 0) {
            throw new NetException("estimateSize overflow");
        }
        int bodySize = request.actualBodySize();
        if (bodySize > config.maxBodySize || bodySize < 0) {
            throw new NetException("packet body size " + bodySize
                    + " exceeds max body size " + config.maxBodySize);
        }
        VersionFactory.getInstance().acquireFence();
    }

    public void releasePermit(WritePacket request) {
        lock.lock();
        try {
            if (!request.acquirePermit) {
                BugLog.getLog().error("current request is not acquired permit");
                return;
            }
            int estimateSize = request.calcMaxPacketSize();
            pendingRequests--;
            if (pendingRequests < 0) {
                BugLog.getLog().error("pendingRequests is " + pendingRequests);
                pendingRequests = 0;
            }
            pendingBytes -= estimateSize;
            if (pendingBytes < 0) {
                BugLog.getLog().error("pendingBytes is " + pendingBytes);
                pendingBytes = 0;
            }
            request.acquirePermit = false;
            pendingReqCond.signalAll();
            pendingBytesCond.signalAll();
        } finally {
            lock.unlock();
        }
    }

    protected void createBizExecutor() {
        if (config.bizThreads > 0) {
            bizExecutor = Executors.newScheduledThreadPool(config.bizThreads,
                    new DtThreadFactory(config.name + "Biz", false));
        }
    }

    void stopWorker(NioWorker worker, DtTime timeout) {
        try {
            if (worker != null) {
                worker.stop(timeout, true);
            }
        } catch (Exception e) {
            log.error("stop worker fail: worker={}, error={}", worker.thread.getName(), e.toString());
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

    protected <T> T waitFuture(CompletableFuture<T> f, DtTime timeout) {
        try {
            return f.get(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            DtUtil.restoreInterruptStatus();
            throw new NetException("interrupted", e);
        } catch (ExecutionException e) {
            throw new NetException("execution exception", e);
        } catch (TimeoutException e) {
            throw new NetTimeoutException("timeout: " + timeout.getTimeout(TimeUnit.MILLISECONDS) + "ms", e);
        }
    }

    public ScheduledExecutorService getBizExecutor() {
        return bizExecutor;
    }
}
