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
package com.github.dtprj.dongting.bench;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.TwoLevelPool;
import com.github.dtprj.dongting.codec.RefBufferDecoder;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.ByteBufferWriteFrame;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.NioServerConfig;
import com.github.dtprj.dongting.net.ReadFrame;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class NioServerBenchmark extends BenchBase {
    private static final DtLog log = DtLogs.getLogger(NioServerBenchmark.class);
    private NioServer server;
    private NioClient client;

    private byte[] data;
    private static final int DATA_LEN = 128;
    private static final boolean SYNC = false;
    private static final int THREAD_COUNT = 1;
    private static final long TIME = 10 * 1000;
    private static final long WARMUP_TIME = 5000;
    private static final long TIMEOUT = 5000;
    private static final int CMD = Commands.CMD_PING;

    public NioServerBenchmark(int threadCount, long testTime, long warmupTime) {
        super(threadCount, testTime, warmupTime);
    }

    @Override
    public void init() {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.setIoThreads(1);
        serverConfig.setBizThreads(0);
        serverConfig.setPort(9000);
        server = new NioServer(serverConfig);
        server.start();

        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        client = new NioClient(clientConfig);
        client.start();
        client.waitStart();

        data = new byte[DATA_LEN];
        new Random().nextBytes(data);
    }

    @Override
    public void shutdown() {
        client.stop(new DtTime(3, TimeUnit.SECONDS));
        server.stop(new DtTime(3, TimeUnit.SECONDS));

        TwoLevelPool direct = (TwoLevelPool) TwoLevelPool.getDefaultFactory().apply(new Timestamp(), true);
        TwoLevelPool heap = (TwoLevelPool) TwoLevelPool.getDefaultFactory().apply(new Timestamp(), false);
        log.info("global direct pool stats: {}", direct.getLargePool().formatStat());
        log.info("global heap pool stats: {}", heap.getLargePool().formatStat());
    }

    @Override
    public void test(int threadIndex, long startTime, int state) {
        try {
            final DtTime timeout = new DtTime(TIMEOUT, TimeUnit.MILLISECONDS);
            ByteBufferWriteFrame req = new ByteBufferWriteFrame(ByteBuffer.wrap(data));
            req.setCommand(CMD);
            CompletableFuture<ReadFrame<RefBuffer>> f = client.sendRequest(req, RefBufferDecoder.PLAIN_INSTANCE, timeout);

            if (SYNC) {
                ReadFrame<RefBuffer> rf = f.get();
                success(state);
                RefBuffer rc = rf.getBody();
                rc.release();
            } else {
                f.handle((result, ex) -> {
                    logRt(startTime, state);
                    if (ex != null) {
                        fail(state);
                    } else {
                        RefBuffer rc = result.getBody();
                        rc.release();
                        success(state);
                    }
                    return null;
                });
            }
        } catch (Exception e) {
            fail(state);
        } finally {
            if(SYNC) {
                logRt(startTime, state);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new NioServerBenchmark(THREAD_COUNT, TIME, WARMUP_TIME).start();
    }
}
