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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author huangli
 */
public class NioClient extends NioNet {

    private static final DtLog log = DtLogs.getLogger(NioClient.class);

    private final NioClientConfig config;
    private final NioWorker worker;

    private final CopyOnWriteArrayList<Peer> peers;
    private List<CompletableFuture<Void>> startFutures;

    public NioClient(NioClientConfig config) {
        super(config);
        this.config = config;
        ArrayList<Peer> list = new ArrayList<>();
        if (config.getHostPorts() != null) {
            for (HostPort hp : config.getHostPorts()) {
                Peer p = new Peer(hp, false);
                list.add(p);
            }
        }
        this.peers = new CopyOnWriteArrayList<>(list);
        this.worker = new NioWorker(nioStatus, config.getName() + "IoWorker", config);
    }

    @Override
    protected void doStart() throws Exception {
        if (peers.size() == 0) {
            throw new IllegalArgumentException("no servers");
        }
        startFutures = new ArrayList<>();
        initBizExecutor();
        worker.start();
        for (Peer peer: peers) {
            startFutures.add(worker.connect(peer));
        }
    }

    public void waitStart() {
        final DtTime t = new DtTime(config.getConnectTimeoutMillis(), TimeUnit.MILLISECONDS);
        for (CompletableFuture<Void> f : startFutures) {
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
        for (CompletableFuture<Void> f : startFutures) {
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
        this.startFutures = null;

        if (successCount == 0) {
            log.error("NioClient [{}] start fail: timeoutPeerCount={},failPeerCount={}", config.getName(), timeoutCount, failCount);
            throw new NetException("init NioClient fail:timeout=" + config.getConnectTimeoutMillis()
                    + "ms, timeoutConnectionCount=" + timeoutCount + ", failConnectionCount=" + failCount);
        } else {
            log.info("NioClient [{}] started: connectPeerCount={}, timeoutPeerCount={}, failPeerCount={}",
                    config.getName(), successCount, timeoutCount, failCount);
        }
    }

    public CompletableFuture<ReadFrame> sendRequest(WriteFrame request, Decoder decoder, DtTime timeout) {
        return sendRequest(worker, null, request, decoder, timeout);
    }

    public CompletableFuture<ReadFrame> sendRequest(Peer peer, WriteFrame request, Decoder decoder, DtTime timeout) {
        return sendRequest(worker, peer, request, decoder, timeout);
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