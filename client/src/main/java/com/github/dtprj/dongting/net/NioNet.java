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
import com.github.dtprj.dongting.common.VersionFactory;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
@SuppressWarnings("Convert2Diamond")
public abstract class NioNet extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(NioNet.class);
    private final NioConfig config;
    final NioStatus nioStatus;
    protected volatile ExecutorService bizExecutor;
    private final PerfCallback perfCallback;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition pendingReqCond = lock.newCondition();
    private final Condition pendingBytesCond = lock.newCondition();
    int pendingRequests;
    int pendingBytes;

    public NioNet(NioConfig config) {
        this.config = config;
        this.nioStatus = new NioStatus();
        this.perfCallback = config.getPerfCallback();
        if (config.getMaxPacketSize() < config.getMaxBodySize() + 128 * 1024) {
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

    <T> void send(NioWorker worker, Peer peer, WritePacket request, Decoder<T> decoder,
                  DtTime timeout, RpcCallback<T> callback) {
        send0(worker, peer, null, request, decoder, timeout, callback);
    }

    <T> void push(DtChannelImpl dtc, WritePacket request, Decoder<T> decoder, DtTime timeout, RpcCallback<T> callback) {
        send0(dtc.workerStatus.getWorker(), null, dtc, request, decoder, timeout, callback);
    }

    private <T> void send0(NioWorker worker, Peer peer, DtChannelImpl dtc, WritePacket request,
                           Decoder<T> decoder, DtTime timeout, RpcCallback<T> callback) {
        try {
            int estimateSize = generalCheck(request, timeout, callback);
            request.setPacketType(decoder != null ? PacketType.TYPE_REQ : PacketType.TYPE_ONE_WAY);

            while (true) {
                VersionFactory.getInstance().acquireFence();
                int maxPending = config.getMaxOutRequests();
                long maxPendingBytes = config.getMaxOutBytes();
                if (maxPending <= 0 && maxPendingBytes <= 0) {
                    break;
                }

                long t = perfCallback.takeTime(PerfConsts.RPC_D_ACQUIRE);
                lock.lock();
                try {
                    if (maxPending > 0 && pendingRequests + 1 > maxPending) {
                        if (!pendingReqCond.await(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)) {
                            throw new NetTimeoutException("too many pending requests, client wait permit timeout in "
                                    + timeout.getTimeout(TimeUnit.MILLISECONDS) + " ms");
                        }
                        // re-check all
                        continue;
                    }
                    if (maxPendingBytes > 0 && pendingBytes + estimateSize > maxPendingBytes) {
                        if (!pendingBytesCond.await(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)) {
                            throw new NetTimeoutException("too many pending bytes, client wait permit timeout in "
                                    + timeout.getTimeout(TimeUnit.MILLISECONDS) + " ms");
                        }
                        // re-check all
                        continue;
                    }
                    pendingRequests++;
                    pendingBytes += estimateSize;
                } finally {
                    lock.unlock();
                    perfCallback.fireTime(PerfConsts.RPC_D_ACQUIRE, t);
                }
                callback = wrapCallback(callback, estimateSize);
                break;
            }
        } catch (Exception e) {
            RpcCallback.callFail(callback, e);
            request.clean();
            return;
        }
        WriteData wd;
        if (peer != null) {
            wd = new WriteData(peer, request, timeout, callback, decoder);
        } else {
            wd = new WriteData(dtc, request, timeout, callback, decoder);
        }
        worker.writeReqInBizThreads(wd);
    }

    private <T> int generalCheck(WritePacket request, DtTime timeout, RpcCallback<T> callback) {
        Objects.requireNonNull(timeout);
        Objects.requireNonNull(callback);
        Objects.requireNonNull(request);
        DtUtil.checkPositive(request.getCommand(), "request.command");

        if (status != STATUS_RUNNING) {
            throw new NetException("error state: " + status);
        }

        // can't invoke actualSize() here because seq and timeout field is not set yet
        int estimateSize = request.calcMaxPacketSize();
        if (estimateSize > config.getMaxPacketSize() || estimateSize < 0) {
            throw new NetException("estimateSize overflow");
        }
        int bodySize = request.actualBodySize();
        if (bodySize > config.getMaxBodySize() || bodySize < 0) {
            throw new NetException("packet body size " + bodySize
                    + " exceeds max body size " + config.getMaxBodySize());
        }
        return estimateSize;
    }

    private <T> RpcCallback<T> wrapCallback(RpcCallback<T> callback, int estimateSize) {
        return new RpcCallback<T>() {
            boolean b;

            @Override
            public void success(ReadPacket<T> resp) {
                RpcCallback.callSuccess(callback, resp);
                updatePending();
            }

            @Override
            public void fail(Throwable ex) {
                RpcCallback.callFail(callback, ex);
                updatePending();
            }

            private void updatePending() {
                if (b) {
                    BugLog.getLog().error("already called update pending");
                }
                b = true;
                lock.lock();
                try {
                    pendingRequests--;
                    pendingBytes -= estimateSize;
                    pendingReqCond.signal();
                    pendingBytesCond.signal();
                } finally {
                    lock.unlock();
                }
            }
        };
    }

    protected void initBizExecutor() {
        if (config.getBizThreads() > 0) {
            bizExecutor = new ThreadPoolExecutor(config.getBizThreads(), config.getBizThreads(),
                    1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
                    new DtThreadFactory(config.getName() + "Biz", false));
        }
        nioStatus.getProcessors().forEach((command, p) -> {
            if (p.useDefaultExecutor) {
                if (bizExecutor != null) {
                    p.executor = bizExecutor;
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
