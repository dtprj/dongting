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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.RefBufferDecoder;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.dtprj.dongting.common.Tick.tick;
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
        serverConfig.setCloseTimeout(3000);
        server = new NioServer(serverConfig);
        if (register != null) {
            register.run();
        }

        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.setCleanInterval(1);
        clientConfig.setSelectTimeout(1);
        clientConfig.setMaxOutRequests(1);
        clientConfig.setCloseTimeout(3000);
        clientConfig.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        client = new NioClient(clientConfig);

        server.start();
        client.start();
        client.waitStart();
    }

    @AfterEach
    public void shutdown() {
        DtUtil.close(client, server);
    }

    private CompletableFuture<?> send(DtTime timeout) {
        ByteBufferWriteFrame wf = new ByteBufferWriteFrame(ByteBuffer.allocate(1));
        wf.setCommand(CMD);
        return client.sendRequest(wf, new RefBufferDecoder(), timeout);
    }

    private void registerDelayPingProcessor(int sleepTime) {
        server.register(CMD, new NioServer.PingProcessor() {
            @Override
            public WriteFrame process(ReadFrame<RefBuffer> frame, ChannelContext channelContext, ReqContext reqContext) {
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
        CompletableFuture<?> f1 = send(new DtTime(1, TimeUnit.SECONDS));
        try {
            CompletableFuture<?> f2 = send(new DtTime(1, TimeUnit.NANOSECONDS));
            f2.get(5, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("too many pending requests"));
        }
        f1.get(5, TimeUnit.SECONDS);
        assertEquals(1, client.semaphore.availablePermits());
        //ensure connection status is correct after timeout
        NioServerClientTest.invoke(client);
    }

    @Test
    public void dropBeforeRequestSendTest() throws Exception {
        setup(() -> registerDelayPingProcessor(0));
        try {
            DtTime deadline = new DtTime(System.nanoTime() - 5 * 1000 * 1000, 1, TimeUnit.NANOSECONDS);
            CompletableFuture<?> f1 = send(deadline);
            f1.get(5, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("timeout before send"));
        }
        assertEquals(1, client.semaphore.availablePermits());
        //ensure connection status is correct after timeout
        NioServerClientTest.invoke(client);
    }

    @Test
    public void processTimeoutTest() throws Exception {
        int oldCount = runCount.get();
        setup(() -> registerDelayPingProcessor(tick(15)));
        try {
            CompletableFuture<?> f = send(new DtTime(tick(14), TimeUnit.MILLISECONDS));
            f.get(5, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("timeout: "), e.getCause().getMessage());
        }
        // wait server process finished
        Thread.sleep(tick(15));

        // need more check server side status
        assertEquals(oldCount + 1, runCount.get());

        assertEquals(1, client.semaphore.availablePermits());

        //ensure connection status is correct after timeout
        NioServerClientTest.invoke(client);
    }

    private void serverTimeoutBeforeProcessTest(boolean runProcessInIoThread) throws Exception {
        ReqProcessor<ByteBuffer> p = new ReqProcessor<>() {
            @Override
            public WriteFrame process(ReadFrame<ByteBuffer> frame, ChannelContext channelContext, ReqContext reqContext) {
                ByteBufferWriteFrame resp = new ByteBufferWriteFrame(frame.getBody());
                resp.setRespCode(CmdCodes.SUCCESS);
                return resp;
            }

            @Override
            public Decoder<ByteBuffer> createDecoder() {
                return new IoFullPackByteBufferDecoder() {
                    @Override
                    public ByteBuffer decode(ByteBuffer buffer) {
                        try {
                            Thread.sleep(tick(15));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return super.decode(buffer);
                    }
                };
            }
        };

        Runnable reg = runProcessInIoThread ? () -> server.register(CMD, p, null) : () -> server.register(CMD, p);
        setup(reg);

        int oldCount = runCount.get();

        try {
            CompletableFuture<?> f = send(new DtTime(tick(10), TimeUnit.MILLISECONDS));
            f.get(5, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("timeout: "), e.getCause().getMessage());
        }
        assertEquals(1, client.semaphore.availablePermits());
        //ensure connection status is correct after timeout
        NioServerClientTest.invoke(client);

        // wait server process finished
        Thread.sleep(tick(15));

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
