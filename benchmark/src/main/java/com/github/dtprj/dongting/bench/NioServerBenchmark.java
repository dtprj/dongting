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

import com.github.dtprj.dongting.buf.RefByteBuffer;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.net.ByteBufferDecoder;
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
    private NioServer server;
    private NioClient client;

    private byte[] data;
    private final int DATA_LEN = 128;
    private static final boolean SYNC = false;
    private static final int THREAD_COUNT = 1;
    private static final long TIME = 30 * 1000;
    private static final long WARMUP_TIME = 5000;
    private static final long TIMEOUT = 5000;

    public NioServerBenchmark(int threadCount, long testTime, long warmupTime) {
        super(threadCount, testTime, warmupTime);
    }

    @Override
    public void init() throws Exception {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.setIoThreads(1);
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
    public void shutdown() throws Exception {
        client.stop();
        server.stop();
    }

    @Override
    public void test(int threadIndex) {
        try {
            final DtTime timeout = new DtTime(TIMEOUT, TimeUnit.MILLISECONDS);
            ByteBufferWriteFrame req = new ByteBufferWriteFrame(ByteBuffer.wrap(data));
            req.setCommand(Commands.CMD_PING);
            CompletableFuture<ReadFrame> f = client.sendRequest(req, new ByteBufferDecoder(128), timeout);

            if (SYNC) {
                ReadFrame rf = f.get();
                successCount.increment();
                RefByteBuffer rc = (RefByteBuffer) rf.getBody();
                rc.release();
            } else {
                f.handle((result, ex) -> {
                    if (ex != null) {
                        failCount.increment();
                    } else {
                        RefByteBuffer rc = (RefByteBuffer) result.getBody();
                        rc.release();
                        successCount.increment();
                    }
                    return null;
                });
            }
        } catch (Exception e) {
            failCount.increment();
        }
    }

    public static void main(String[] args) throws Exception {
        new NioServerBenchmark(THREAD_COUNT, TIME, WARMUP_TIME).start();
    }
}
