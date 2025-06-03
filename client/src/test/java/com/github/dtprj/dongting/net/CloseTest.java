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
import com.github.dtprj.dongting.codec.RefBufferDecoderCallback;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.TestUtil;
import com.github.dtprj.dongting.common.Tick;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class CloseTest {
    private static final int CMD = 2000;
    private NioServer server;
    private NioClient client;
    private volatile boolean received;

    private void setup(int sleepTime, boolean finishWhenClose, int cleanInterval) {
        received = false;
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.port = 9000;
        server = new NioServer(serverConfig);
        server.register(CMD, new NioServer.PingProcessor() {
            @Override
            public WritePacket process(ReadPacket<RefBuffer> packet, ReqContext reqContext) {
                received = true;
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return super.process(packet, reqContext);
            }
        });

        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.cleanInterval = cleanInterval;
        clientConfig.finishPendingImmediatelyWhenChannelClose = finishWhenClose;
        clientConfig.hostPorts = Collections.singletonList(new HostPort("127.0.0.1", 9000));
        client = new NioClient(clientConfig);

        server.start();
        client.start();
        client.waitStart(new DtTime(1, TimeUnit.SECONDS));
    }

    @AfterEach
    public void shutdown() {
        TestUtil.stop(client, server);
    }

    @Test
    public void testCleanInterval() {
        setup(Tick.tick(30), false, 1);

        ByteBufferWritePacket wf = new ByteBufferWritePacket(ByteBuffer.allocate(1));
        wf.setCommand(CMD);
        CompletableFuture<ReadPacket<RefBuffer>> f = new CompletableFuture<>();
        client.sendRequest(wf, ctx -> new RefBufferDecoderCallback(),
                new DtTime(10, TimeUnit.SECONDS), RpcCallback.fromFuture(f));

        TestUtil.waitUtil(() -> received);

        Peer p = client.getPeers().get(0);
        client.disconnect(p);
        try {
            f.get(10, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getMessage().contains("channel closed, future cancelled by timeout cleaner"), e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testCleanWhenClose() throws Exception {
        setup(Tick.tick(30), true, 1000000);

        ByteBufferWritePacket wf = new ByteBufferWritePacket(ByteBuffer.allocate(1));
        wf.setCommand(CMD);
        CompletableFuture<ReadPacket<RefBuffer>> f = new CompletableFuture<>();
        client.sendRequest(wf, ctx -> new RefBufferDecoderCallback(),
                new DtTime(10, TimeUnit.SECONDS), RpcCallback.fromFuture(f));

        TestUtil.waitUtil(() -> received);

        Peer p = client.getPeers().get(0);
        client.disconnect(p).get(10, TimeUnit.SECONDS);
        assertEquals(0, client.worker.pendingOutgoingRequests.size());
        try {
            f.get(10, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getMessage().contains("channel closed, cancel pending request in NioWorker"), e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }
}
