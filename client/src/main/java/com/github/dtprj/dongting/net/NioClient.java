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
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
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
                Peer p = new Peer(hp, this);
                list.add(p);
            }
        }
        this.peers = new CopyOnWriteArrayList<>(list);
        this.worker = new NioWorker(nioStatus, config.getName() + "IoWorker", config);
    }

    @Override
    protected void doStart() {
        if (peers.size() == 0) {
            throw new IllegalArgumentException("no servers");
        }
        startFutures = new ArrayList<>();
        initBizExecutor();
        worker.start();
        for (Peer peer : peers) {
            startFutures.add(worker.connect(peer));
        }
    }

    public void waitStart() {
        final DtTime t = new DtTime(config.getWaitStartTimeoutMillis(), TimeUnit.MILLISECONDS);
        int successCount = 0;
        int timeoutCount = 0;
        int failCount = 0;
        List<Peer> peers = this.peers;
        StringBuilder sb = new StringBuilder(startFutures.size() * 32);
        sb.append("peer status:\n");
        for (int i = 0; i < peers.size(); i++) {
            Peer peer = peers.get(i);
            sb.append(peer.getEndPoint()).append(' ');
            CompletableFuture<Void> f = startFutures.get(i);
            if (!f.isDone()) {
                long restMillis = t.rest(TimeUnit.MILLISECONDS);
                if (restMillis > 0) {
                    try {
                        f.get(restMillis, TimeUnit.MILLISECONDS);
                        sb.append("connected\n");
                        successCount++;
                    } catch (InterruptedException e) {
                        ThreadUtils.restoreInterruptStatus();
                        sb.append("interrupted\n");
                        failCount++;
                    }  catch (ExecutionException e) {
                        sb.append("connect fail: ").append(e).append('\n');
                        failCount++;
                    } catch (TimeoutException e) {
                        sb.append("timeout\n");
                        timeoutCount++;
                    }
                } else {
                    sb.append("timeout\n");
                    timeoutCount++;
                }
            } else {
                try {
                    f.getNow(null);
                    sb.append("connected\n");
                    successCount++;
                } catch (CompletionException e) {
                    sb.append("connect fail: ").append(e).append('\n');
                    failCount++;
                }
            }
        }

        if (successCount == 0) {
            log.error("[{}] start fail: timeoutPeerCount={},failPeerCount={}\n{}",
                    config.getName(), timeoutCount, failCount, sb);
            throw new NetException("init NioClient fail:timeout=" + config.getWaitStartTimeoutMillis()
                    + "ms, timeoutConnectionCount=" + timeoutCount + ", failConnectionCount=" + failCount);
        } else {
            log.info("[{}] started: connectPeerCount={}, timeoutPeerCount={}, failPeerCount={}\n{}",
                    config.getName(), successCount, timeoutCount, failCount, sb);
        }
        this.startFutures = null;
    }

    public CompletableFuture<ReadFrame> sendRequest(WriteFrame request, Decoder decoder, DtTime timeout) {
        return sendRequest(worker, null, request, decoder, timeout);
    }

    public CompletableFuture<ReadFrame> sendRequest(Peer peer, WriteFrame request, Decoder decoder, DtTime timeout) {
        return sendRequest(worker, peer, request, decoder, timeout);
    }

    @Override
    protected void doStop() {
        DtTime timeout = new DtTime(config.getCloseTimeoutMillis(), TimeUnit.MILLISECONDS);
        worker.preStop();
        try {
            long rest = timeout.rest(TimeUnit.MILLISECONDS);
            if (rest > 0) {
                worker.getPreCloseFuture().get(rest, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            ThreadUtils.restoreInterruptStatus();
        } catch (TimeoutException e) {
            // ignore
        } catch (ExecutionException e) {
            BugLog.log(e);
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

    public CopyOnWriteArrayList<Peer> getPeers() {
        return peers;
    }

    public CompletableFuture<Void> reconnect(Peer peer) {
        if (peer.getOwner() != this) {
            throw new IllegalArgumentException("the peer is not owned by this client");
        }
        return worker.connect(peer);
    }
}