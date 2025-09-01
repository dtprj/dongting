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
package com.github.dtprj.dongting.bench.rpc;

import com.github.dtprj.dongting.bench.common.BenchBase;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.RefBufferDecoderCallback;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.net.ByteBufferWritePacket;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.NioServerConfig;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class RpcBenchmark extends BenchBase {
    private NioServer server;
    private NioClient client;
    private final int cmd;
    private byte[] data;

    private static final int DATA_LEN = 128;
    private static final boolean SYNC = false;
    private static final long TIMEOUT = 1500;
    private static final boolean PERF = false;

    public static void main(String[] args) throws Exception {
        RpcBenchmark benchmark = new RpcBenchmark(1, 5000, 1000, Commands.CMD_PING);
        benchmark.setLogRt(true);
        benchmark.start();
    }

    public RpcBenchmark(int threadCount, long testTime, long warmupTime, int cmd) {
        super(threadCount, testTime, warmupTime);
        this.cmd = cmd;
    }

    @Override
    public void init() {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.ioThreads = 1;
        serverConfig.bizThreads = 1;
        serverConfig.port = 9000;
        if (PERF) {
            serverConfig.perfCallback = new RpcPerfCallback(true, "server_");
        }
        server = new NioServer(serverConfig);
        server.start();

        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.hostPorts = Collections.singletonList(new HostPort("127.0.0.1", 9000));

        if (PERF) {
            clientConfig.perfCallback = new RpcPerfCallback(true, "client_");
        }

        client = new NioClient(clientConfig);
        client.start();
        client.waitStart(new DtTime(1, TimeUnit.SECONDS));

        data = new byte[DATA_LEN];
        new Random().nextBytes(data);
    }

    @Override
    protected void afterWarmup() {
        PerfCallback c = server.getConfig().perfCallback;
        if (c instanceof RpcPerfCallback) {
            ((RpcPerfCallback) c).start();
        }
        c = client.getConfig().perfCallback;
        if (c instanceof RpcPerfCallback) {
            ((RpcPerfCallback) c).start();
        }
    }

    @Override
    public void shutdown() {
        client.stop(new DtTime(3, TimeUnit.SECONDS));
        server.stop(new DtTime(3, TimeUnit.SECONDS));

        // TwoLevelPool direct = (TwoLevelPool) TwoLevelPool.getDefaultFactory().apply(new Timestamp(), true);
        // TwoLevelPool heap = (TwoLevelPool) TwoLevelPool.getDefaultFactory().apply(new Timestamp(), false);
        // log.info("global direct pool stats: {}", direct.getLargePool().formatStat());
        // log.info("global heap pool stats: {}", heap.getLargePool().formatStat());

        PerfCallback c = client.getConfig().perfCallback;
        if (c instanceof RpcPerfCallback) {
            System.out.println("-----------------------client rpc stats----------------------");
            ((RpcPerfCallback) c).printStats();
            System.out.println("-------------------------------------------------------------");
        }
        c = server.getConfig().perfCallback;
        if (c instanceof RpcPerfCallback) {
            System.out.println("-----------------------server rpc stats----------------------");
            ((RpcPerfCallback) c).printStats();
            System.out.println("-------------------------------------------------------------");
        }
    }

    @Override
    public void test(int threadIndex, long startTime, int state) {
        try {
            final DtTime timeout = new DtTime(TIMEOUT, TimeUnit.MILLISECONDS);
            ByteBufferWritePacket req = new ByteBufferWritePacket(ByteBuffer.wrap(data));
            req.command = cmd;

            if (SYNC) {
                ReadPacket<RefBuffer> rf = client.sendRequest(req,
                        ctx -> new RefBufferDecoderCallback(true), timeout);
                success(state);
                RefBuffer rc = rf.getBody();
                rc.release();
            } else {
                RpcCallback<RefBuffer> c = (resp, ex) -> {
                    if (ex == null) {
                        logRt(startTime, state);
                        RefBuffer rc = resp.getBody();
                        rc.release();
                        success(state);
                    } else {
                        logRt(startTime, state);
                        fail(state);
                    }
                };
                client.sendRequest(req, ctx -> new RefBufferDecoderCallback(true), timeout, c);
            }
        } catch (Exception e) {
            fail(state);
        } finally {
            if (SYNC) {
                logRt(startTime, state);
            }
        }
    }
}
