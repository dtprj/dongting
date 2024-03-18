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
import com.github.dtprj.dongting.common.MockDtTime;
import com.github.dtprj.dongting.common.TestUtil;
import com.github.dtprj.dongting.log.BugLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

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
        clientConfig.setCleanInterval(1);
        clientConfig.setSelectTimeout(1);
        clientConfig.setMaxOutRequests(1);
        clientConfig.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        client = new NioClient(clientConfig);

        server.start();
        client.start();
        client.waitStart();
    }

    @AfterEach
    public void shutdown() {
        TestUtil.stop(client, server);
    }

    private CompletableFuture<?> send(DtTime timeout) {
        ByteBufferWriteFrame wf = new ByteBufferWriteFrame(ByteBuffer.allocate(1));
        wf.setCommand(CMD);
        return client.sendRequest(wf, RefBufferDecoder.INSTANCE, timeout);
    }

    private void registerDelayPingProcessor(CountDownLatch latch1, CountDownLatch latch2) {
        server.register(CMD, new NioServer.PingProcessor() {
            @Override
            public WriteFrame process(ReadFrame<RefBuffer> frame, ChannelContext channelContext, ReqContext reqContext) {
                if (latch1 != null) {
                    latch1.countDown();
                }
                if (latch2 != null) {
                    try {
                        latch2.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                runCount.incrementAndGet();
                return super.process(frame, channelContext, reqContext);
            }
        });
    }

    @Test
    public void acquireTimeoutTest() throws Exception {
        CountDownLatch latch2 = new CountDownLatch(1);
        setup(() -> registerDelayPingProcessor(null, latch2));
        CompletableFuture<?> f1 = send(new DtTime(1, TimeUnit.SECONDS));
        try {
            CompletableFuture<?> f2 = send(new DtTime(1, TimeUnit.NANOSECONDS));
            f2.get(5, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("too many pending requests"));
        }
        latch2.countDown();
        f1.get(5, TimeUnit.SECONDS);
        assertEquals(1, client.semaphore.availablePermits());
        //ensure connection status is correct after timeout
        NioServerClientTest.invoke(client);
    }

    @Test
    public void dropBeforeRequestSendTest() throws Exception {
        setup(() -> registerDelayPingProcessor(null, null));
        BugLog.BUG = false;
        for (int i = 0; i < 3; i++) {
            try {
                DtTime deadline = new DtTime(System.nanoTime() - Duration.ofSeconds(1).toNanos(), 1, TimeUnit.NANOSECONDS);
                CompletableFuture<?> f1 = send(deadline);
                f1.get(5, TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                assertEquals(NetTimeoutException.class, e.getCause().getClass());
                assertTrue(e.getCause().getMessage().contains("timeout before send"), e.getCause().getMessage());
            }
        }
        assertFalse(BugLog.BUG);
        assertEquals(1, client.semaphore.availablePermits());
        //ensure connection status is correct after timeout
        NioServerClientTest.invoke(client);
    }

    @Test
    public void processTimeoutTest() throws Exception {
        int oldCount = runCount.get();
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        setup(() -> registerDelayPingProcessor(latch1, latch2));
        try {
            MockDtTime dtTime = new MockDtTime(1, TimeUnit.SECONDS);
            CompletableFuture<?> f = send(dtTime);

            // make sure server receive the request
            latch1.await();

            dtTime.markTimeout();
            f.get(5, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("request is timeout: "), e.getCause().getMessage());
        }
        // wait server process finished
        latch2.countDown();

        // need more check server side status
        TestUtil.waitUtil(() -> runCount.get() == oldCount + 1);

        assertEquals(1, client.semaphore.availablePermits());

        //ensure connection status is correct after timeout
        NioServerClientTest.invoke(client);
    }

    private void serverTimeoutBeforeProcessTest(boolean runProcessInIoThread) throws Exception {
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        AtomicInteger processCount = new AtomicInteger();
        ReqProcessor<ByteBuffer> p = new ReqProcessor<>() {
            @Override
            public WriteFrame process(ReadFrame<ByteBuffer> frame, ChannelContext channelContext, ReqContext reqContext) {
                ByteBufferWriteFrame resp = new ByteBufferWriteFrame(frame.getBody());
                resp.setRespCode(CmdCodes.SUCCESS);
                processCount.incrementAndGet();
                return resp;
            }

            @Override
            public Decoder<ByteBuffer> createDecoder(int command) {
                return new IoFullPackByteBufferDecoder() {
                    @Override
                    public ByteBuffer decode(ByteBuffer buffer) {
                        latch1.countDown();
                        try {
                            latch2.await();
                            server.workers[0].workerStatus.getTs().refresh(0);
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

        try {
            MockDtTime dtTime = new MockDtTime(1, TimeUnit.SECONDS);
            CompletableFuture<?> f = send(dtTime);
            latch1.await();
            dtTime.markTimeout();
            f.get(5, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("request is timeout: "), e.getCause().getMessage());
        }
        assertEquals(1, client.semaphore.availablePermits());

        latch2.countDown();

        TestUtil.waitUtil(() -> processCount.get() == 1);

        //ensure connection status is correct after timeout
        NioServerClientTest.invoke(client);

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
