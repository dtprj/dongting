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

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtThreadFactory;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.ThreadUtils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public abstract class NioRemoting extends AbstractLifeCircle {
    private final NioConfig config;
    private final Semaphore semaphore;
    protected final NioStatus nioStatus = new NioStatus();
    protected ExecutorService bizExecutor;

    public NioRemoting(NioConfig config) {
        this.config = config;
        this.semaphore = new Semaphore(config.getMaxRequestPending());
        nioStatus.setRequestSemaphore(semaphore);
    }

    public void register(int cmd, CmdProcessor processor) {
        nioStatus.setProcessor(cmd, processor);
    }

    protected CompletableFuture<Frame> sendRequest(DtChannel dtc, Frame request, DtTime timeout) {
        try {
            if (status != LifeStatus.running) {
                return errorFuture(new IllegalStateException("error state: " + status));
            }

            boolean acquire = this.semaphore.tryAcquire(timeout.rest(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            if (acquire) {
                CompletableFuture<Frame> future = new CompletableFuture<>();
                dtc.writeReq(request, timeout, future);
                return future;
            } else {
                return errorFuture(new RemotingException("too many pending requests"));
            }
        } catch (InterruptedException e) {
            return errorFuture(e);
        } catch (Throwable e) {
            return errorFuture(new RemotingException("submit task error", e));
        }
    }

    protected <T> CompletableFuture<T> errorFuture(Throwable e) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(e);
        return f;
    }

    protected void initBizExecutor() {
        // TODO back pressure
        if (config.getBizThreads() > 0) {
            bizExecutor = new ThreadPoolExecutor(config.getBizThreads(), config.getBizQueueSize(),
                    1, TimeUnit.MINUTES, new ArrayBlockingQueue<>(config.getBizQueueSize()),
                    new DtThreadFactory(config.getName() + "Biz", false));
            nioStatus.setBizExecutor(bizExecutor);
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
                    bizExecutor.awaitTermination(rest, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    ThreadUtils.restoreInterruptStatus();
                }
            }
        }
    }
}
