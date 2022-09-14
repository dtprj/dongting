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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.ThreadUtils;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author huangli
 */
public class NioClient extends NioNet {

    private static final DtLog log = DtLogs.getLogger(NioClient.class);

    private final NioClientConfig config;
    private final NioWorker worker;
    private int invokeIndex;

    public NioClient(NioClientConfig config) {
        super(config);
        this.config = config;
        if (config.getHostPorts() == null || config.getHostPorts().size() == 0) {
            throw new IllegalArgumentException("no servers");
        }
        worker = new NioWorker(nioStatus, config.getName() + "IoWorker", config);
    }

    @Override
    protected void doStart() throws Exception {
        worker.start();
        final DtTime t = new DtTime(config.getConnectTimeoutMillis(), TimeUnit.MILLISECONDS);
        final ArrayList<CompletableFuture<DtChannel>> futures = new ArrayList<>();
        for (HostPort hp : config.getHostPorts()) {
            InetSocketAddress addr = new InetSocketAddress(hp.getHost(), hp.getPort());
            CompletableFuture<DtChannel> f = worker.connect(addr);
            futures.add(f);
        }
        for (CompletableFuture<DtChannel> f : futures) {
            long restMillis = t.rest(TimeUnit.MILLISECONDS);
            if (restMillis > 0 && !Thread.currentThread().isInterrupted()) {
                try {
                    f.get(restMillis, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        int successCount = 0;
        int timeoutCount = 0;
        int failCount = 0;
        for (CompletableFuture<DtChannel> f : futures) {
            if (f.isDone()) {
                if (f.isCompletedExceptionally() || f.isCancelled()) {
                    failCount++;
                } else {
                    successCount++;
                }
            } else {
                timeoutCount++;
            }
        }
        if (successCount == 0) {
            throw new NetException("init NioClient fail:timeout=" + config.getConnectTimeoutMillis()
                    + "ms, timeoutConnectionCount=" + timeoutCount + ", failConnectionCount=" + failCount);
        }
        initBizExecutor();
    }

    public CompletableFuture<ReadFrame> sendRequest(WriteFrame request, Decoder decoder, DtTime timeout) {
        if (status != LifeStatus.running) {
            return errorFuture(new IllegalStateException("error state: " + status));
        }
        List<DtChannel> channels = worker.getChannels();
        DtChannel dtc;
        try {
            int size = channels.size();
            if (size > 0) {
                dtc = channels.get(invokeIndex++ % size);
            } else {
                return errorFuture(new NetException("no available servers"));
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            try {
                dtc = channels.get(0);
            } catch (ArrayIndexOutOfBoundsException e2) {
                return errorFuture(new NetException("no available servers"));
            }
        }
        return sendRequest(dtc, request, decoder, timeout);
    }

    @Override
    protected void doStop() throws Exception {
        DtTime timeout = new DtTime(config.getCloseTimeoutMillis(), TimeUnit.MILLISECONDS);
        worker.preStop();
        try {
            long rest = timeout.rest(TimeUnit.MILLISECONDS);
            if (rest > 0) {
                worker.getPreCloseFuture().get(rest, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            ThreadUtils.restoreInterruptStatus();
        } catch (TimeoutException e){
            // ignore
        }
        worker.stop();
        try {
            long rest = timeout.rest(TimeUnit.MILLISECONDS);
            if (rest > 0) {
                worker.getThread().join(rest);
            }
        } catch (InterruptedException e) {
            ThreadUtils.restoreInterruptStatus();
        }
        shutdownBizExecutor(timeout);
    }

}