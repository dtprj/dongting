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
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class NioClient extends AbstractLifeCircle {

    private static final DtLog log = DtLogs.getLogger(NioClient.class);

    private final NioClientConfig config;
    private final NioWorker worker;
    private final NioStatus nioStatus = new NioStatus();
    private final CopyOnWriteArrayList<CompletableFuture<DtChannel>> futures = new CopyOnWriteArrayList<>();

    public NioClient(NioClientConfig config) {
        this.config = config;
        if (config.getHostPorts() == null || config.getHostPorts().size() == 0) {
            throw new IllegalArgumentException("no servers");
        }
        worker = new NioWorker(nioStatus, config.getName() + "IoWorker");
    }

    @Override
    protected void doStart() throws Exception {
        worker.start();
        DtTime t = new DtTime(config.getConnectTimeoutMillis(), TimeUnit.MILLISECONDS);
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
            // TODO error message
            throw new RemotingException();
        }
    }

    @Override
    protected void doStop() throws Exception {
        worker.stop();
    }

    public void register(int cmd, CmdProcessor processor) {
        nioStatus.setProcessor(cmd, processor);
    }
}
