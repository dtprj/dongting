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

import com.github.dtprj.dongting.common.CloseUtil;
import com.github.dtprj.dongting.common.DtTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
public class TimeoutTest {

    private static final int CMD = 2000;

    private NioServer server;
    private NioClient client;
    private final AtomicInteger runCount = new AtomicInteger();

    private void setup(Runnable register) {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.setPort(9000);
        server = new NioServer(serverConfig);
        if (register != null) {
            register.run();
        }

        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.setCleanIntervalMills(1);
        clientConfig.setSelectTimeoutMillis(1);
        clientConfig.setMaxOutRequests(1);
        clientConfig.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        client = new NioClient(clientConfig);

        server.start();
        client.start();
        client.waitStart();
    }

    @AfterEach
    public void shutdown(){
        CloseUtil.close(client, server);
    }

    private CompletableFuture<ReadFrame> send(DtTime timeout) {
        ByteBufferWriteFrame wf = new ByteBufferWriteFrame(ByteBuffer.allocate(1));
        wf.setCommand(CMD);
        wf.setFrameType(FrameType.TYPE_REQ);
        return client.sendRequest(wf, new ByteBufferDecoder(0), timeout);
    }

    private void registerDelayPingProcessor(int sleepTime) {
        server.register(CMD, new NioServer.PingProcessor() {
            @Override
            public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                runCount.incrementAndGet();
                return super.process(frame, channelContext, reqContext);
            }
        });
    }

    @Test
    public void acquireTimeoutTest() throws Exception {
        setup(() -> registerDelayPingProcessor(10));
        CompletableFuture<ReadFrame> f1 = send(new DtTime(1, TimeUnit.SECONDS));
        try {
            CompletableFuture<ReadFrame> f2 = send(new DtTime(1, TimeUnit.NANOSECONDS));
            f2.get(1, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("too many pending requests"));
        }
        f1.get(1, TimeUnit.SECONDS);
        assertEquals(1, client.nioStatus.getRequestSemaphore().availablePermits());
        //ensure connection status is correct after timeout
        NioServerClientTest.invoke(client);
    }

    @Test
    public void dropBeforeRequestSendTest() throws Exception {
        setup(() -> registerDelayPingProcessor(10));
        try {
            CompletableFuture<ReadFrame> f1 = send(new DtTime(1, TimeUnit.NANOSECONDS));
            f1.get(1, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("timeout before send"));
        }
        assertEquals(1, client.nioStatus.getRequestSemaphore().availablePermits());
        //ensure connection status is correct after timeout
        NioServerClientTest.invoke(client);
    }

    @Test
    public void processTimeoutTest() throws Exception {
        int oldCount = runCount.get();
        setup(() -> registerDelayPingProcessor(15));
        try {
            CompletableFuture<ReadFrame> f = send(new DtTime(14, TimeUnit.MILLISECONDS));
            f.get(1, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("timeout: "), e.getCause().getMessage());
        }
        assertEquals(1, client.nioStatus.getRequestSemaphore().availablePermits());
        //ensure connection status is correct after timeout
        NioServerClientTest.invoke(client);

        // wait server process finished
        Thread.sleep(15);

        // need more check server side status
        assertEquals(oldCount + 1, runCount.get());
    }

    private void serverTimeoutBeforeProcessTest(boolean runProcessInIoThread) throws Exception {
        ReqProcessor p = new ReqProcessor() {
            @Override
            public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
                ByteBufferWriteFrame resp = new ByteBufferWriteFrame((ByteBuffer) frame.getBody());
                resp.setRespCode(CmdCodes.SUCCESS);
                return resp;
            }

            @Override
            public Decoder getDecoder() {
                return new IoFullPackByteBufferDecoder() {
                    @Override
                    public Object decode(ChannelContext context, ByteBuffer buffer, int bodyLen, boolean start, boolean end) {
                        try {
                            Thread.sleep(15);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return super.decode(context, buffer, bodyLen, start, end);
                    }
                };
            }
        };

        Runnable reg = runProcessInIoThread ? () -> server.register(CMD, p, null) : () -> server.register(CMD, p);
        setup(reg);

        int oldCount = runCount.get();

        try {
            CompletableFuture<ReadFrame> f = send(new DtTime(10, TimeUnit.MILLISECONDS));
            f.get(1, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("timeout: "), e.getCause().getMessage());
        }
        assertEquals(1, client.nioStatus.getRequestSemaphore().availablePermits());
        //ensure connection status is correct after timeout
        NioServerClientTest.invoke(client);

        // wait server process finished
        Thread.sleep(15);

        // need more check server side status
        assertEquals(oldCount, runCount.get());
    }

    @Test
    public void serverTimeoutBeforeBizProcessTest() throws Exception {
        serverTimeoutBeforeProcessTest(false);
    }

    @Test
    public void serverTimeoutBeforeIoProcessTest() throws Exception {
        serverTimeoutBeforeProcessTest(true);
    }
}
