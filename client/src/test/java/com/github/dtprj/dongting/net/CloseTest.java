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
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.RefBufferDecoderCallback;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.TestUtil;
import com.github.dtprj.dongting.test.Tick;
import com.github.dtprj.dongting.test.WaitUtil;
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
    // body must exceed MAX_BUFFER_SIZE so the client is still in the middle of it when channel closes
    private static final int BROKEN_RESP_BODY_SIZE = IoChannelQueue.MAX_BUFFER_SIZE * 2;
    private static final int BROKEN_RESP_ENCODE_FAIL_SIZE = IoChannelQueue.MAX_BUFFER_SIZE * 3 / 2;

    private NioServer server;
    private NioClient client;
    private volatile boolean received;

    private void setup(Runnable register, int cleanInterval) {
        received = false;
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.port = 9000;
        server = new NioServer(serverConfig);
        register.run();

        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.cleanInterval = cleanInterval;
        clientConfig.maxOutRequests = 1;
        clientConfig.maxOutBytes = 5000;
        clientConfig.hostPorts = Collections.singletonList(new HostPort("127.0.0.1", 9000));
        client = new NioClient(clientConfig);

        server.start();
        client.start();
        client.waitStart(new DtTime(1, TimeUnit.SECONDS));
    }

    private void registerDelayPingProcessor(int sleepTime) {
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
    }

    @AfterEach
    public void shutdown() {
        TestUtil.stop(client, server);
    }

    @Test
    public void testCleanWhenClose() throws Exception {
        setup(() -> registerDelayPingProcessor(Tick.tick(30)), 1000000);

        ByteBufferWritePacket wf = new ByteBufferWritePacket(ByteBuffer.allocate(1));
        wf.command = CMD;
        CompletableFuture<ReadPacket<RefBuffer>> f = new CompletableFuture<>();
        client.sendRequest(wf, ctx -> new RefBufferDecoderCallback(),
                new DtTime(10, TimeUnit.SECONDS), RpcCallback.fromFuture(f));

        WaitUtil.waitUtil(() -> received);

        Peer p = client.getPeers().get(0);
        client.disconnect(p).get(10, TimeUnit.SECONDS);
        assertEquals(0, client.worker.workerStatus.pendingReqSize());
        try {
            f.get(10, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getMessage().contains("channel closed, cancel pending request in NioWorker"), e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testCleanWhenCloseDuringRespDecode() throws Exception {
        setup(() -> server.register(CMD, new NioServer.PingProcessor() {
            @Override
            public WritePacket process(ReadPacket<RefBuffer> packet, ReqContext reqContext) {
                BrokenRespPacket resp = new BrokenRespPacket();
                resp.respCode = CmdCodes.SUCCESS;
                return resp;
            }
        }), 100);

        CompletableFuture<ReadPacket<RefBuffer>> f = new CompletableFuture<>();
        ByteBufferWritePacket wf = new ByteBufferWritePacket(ByteBuffer.allocate(1));
        wf.command = CMD;
        client.sendRequest(wf, ctx -> new RefBufferDecoderCallback(),
                new DtTime(10, TimeUnit.SECONDS), RpcCallback.fromFuture(f));
        try {
            f.get(10, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetException.class, e.getCause().getClass());
            assertEquals("channel close", e.getCause().getMessage());
        }
        // callFail releases the permit before it completes the future
        assertEquals(0, client.nioStatus.outPendingRequests);
        assertEquals(0, client.nioStatus.outPendingBytes);

        Peer p = client.getPeers().get(0);
        WaitUtil.waitUtil(() -> p.dtChannel == null);
        WaitUtil.waitUtil(() -> p.status == PeerStatus.connected);
        NioServerClientTest.invoke(client);
    }

    private static class BrokenRespPacket extends WritePacket {
        private final ByteBuffer body = ByteBuffer.allocate(BROKEN_RESP_BODY_SIZE);
        private int encodedBytes;

        @Override
        protected int calcActualBodySize() {
            return body.remaining();
        }

        @Override
        protected boolean encodeBody(EncodeContext context, ByteBuffer dest) {
            int n = Math.min(dest.remaining(), body.remaining());
            int oldLimit = body.limit();
            body.limit(body.position() + n);
            dest.put(body);
            body.limit(oldLimit);
            encodedBytes += n;
            if (encodedBytes > BROKEN_RESP_ENCODE_FAIL_SIZE) {
                throw new NetException("mock: response body encode fail");
            }
            return !body.hasRemaining();
        }
    }
}
