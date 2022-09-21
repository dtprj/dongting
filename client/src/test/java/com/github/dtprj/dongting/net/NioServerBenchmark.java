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

import com.github.dtprj.dongting.bench.BenchBase;
import com.github.dtprj.dongting.common.DtTime;

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
    private final int DATA_LEN = 5;

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
            final DtTime timeout = new DtTime(1, TimeUnit.SECONDS);
            ByteBufferWriteFrame req = new ByteBufferWriteFrame();
            req.setFrameType(CmdType.TYPE_REQ);
            req.setCommand(Commands.CMD_PING);
            req.setBody(ByteBuffer.wrap(data));
            CompletableFuture<ReadFrame> f = client.sendRequest(req, ByteBufferDecoder.INSTANCE, timeout);

//            f.get();
//            successCount.increment();

            f.handle((result, ex) -> {
                if (ex != null) {
                    failCount.increment();
                } else {
                    successCount.increment();
                }
                return null;
            });
        } catch (Exception e) {
            failCount.increment();
        }
    }

    public static void main(String[] args) throws Exception {
        new NioServerBenchmark(128, 10000, 1000).start();
    }
}
