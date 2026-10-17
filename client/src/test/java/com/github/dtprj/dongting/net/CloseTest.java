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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    public void testCleanWaitingConnectListReentry() throws Exception {
        setup(() -> { }, 1000000);
        Peer peer = client.getPeers().get(0);
        CompletableFuture<Void> removeFuture = new CompletableFuture<>();
        runInIoThread(() -> {
            int[] failCount = {0};
            ByteBufferWritePacket wf1 = new ByteBufferWritePacket(null);
            wf1.packetType = PacketType.TYPE_REQ;
            PacketInfoReq r1 = new PacketInfoReq(null, peer, null, wf1, new DtTime(10, TimeUnit.SECONDS),
                    (resp, ex) -> {
                        failCount[0]++;
                        client.removePeer(peer).whenComplete((v, e) -> {
                            if (e != null) {
                                removeFuture.completeExceptionally(e);
                            } else {
                                removeFuture.complete(v);
                            }
                        });
                    }, null);
            ByteBufferWritePacket wf2 = new ByteBufferWritePacket(null);
            wf2.packetType = PacketType.TYPE_REQ;
            PacketInfoReq r2 = new PacketInfoReq(null, peer, null, wf2, new DtTime(10, TimeUnit.SECONDS),
                    (resp, ex) -> failCount[0]++, null);
            peer.addToWaitConnectList(r1);
            peer.addToWaitConnectList(r2);
            peer.cleanWaitingConnectList(wd -> new NetException("test"));
            assertEquals(2, failCount[0]);
            assertTrue(wf1.cleaned && wf2.cleaned);
        });
        removeFuture.get(10, TimeUnit.SECONDS);
        runInIoThread(() -> {
            assertEquals(PeerStatus.removed, peer.status);
            assertNull(peer.dtChannel);
        });
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
        DtChannelImpl oldChannel = client.getPeers().get(0).dtChannel;

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
        WaitUtil.waitUtil(() -> p.status == PeerStatus.connected && p.dtChannel != null && p.dtChannel != oldChannel);
        NioServerClientTest.invoke(client);
    }

    private void runInIoThread(Runnable task) throws Exception {
        CompletableFuture.runAsync(task, r -> client.worker.doInIoThread(r, null)).get(10, TimeUnit.SECONDS);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCloseFromReadCallback(boolean decodeFail) throws Exception {
        setup(() -> { }, 1000000);
        CompletableFuture<Boolean> deferred = new CompletableFuture<>();
        DtChannelImpl dtc = client.getPeers().get(0).dtChannel;
        runInIoThread(() -> {
            dtc.peer.shouldAutoReconnect = false;
            ByteBufferWritePacket req = new ByteBufferWritePacket(null);
            req.packetType = PacketType.TYPE_REQ;
            req.command = CMD;
            req.seq = dtc.getAndIncSeq();
            PacketInfoReq pi = new PacketInfoReq(null, null, dtc, req, new DtTime(10, TimeUnit.SECONDS),
                    (resp, ex) -> {
                        client.worker.close(dtc);
                        if (resp != null) {
                            resp.clean();
                        }
                        deferred.complete(dtc.encodingOrDecoding && !dtc.isClosed() && (decodeFail == (ex != null)));
                    }, ctx -> new RefBufferDecoderCallback() {
                        @Override
                        public void doDecode(ByteBuffer buffer, int bodyLen, int currentPos) {
                            if (decodeFail) {
                                throw new NetException("mock decode failure");
                            }
                            super.doDecode(buffer, bodyLen, currentPos);
                        }
                    });
            dtc.workerStatus.addPendingReq(pi);
            ByteBufferWritePacket resp = new ByteBufferWritePacket(ByteBuffer.allocate(1));
            resp.packetType = PacketType.TYPE_RESP;
            resp.command = CMD;
            resp.seq = req.seq;
            ByteBuffer buf = ByteBuffer.allocate(resp.actualSize());
            assertTrue(resp.encode(new EncodeContext(dtc.workerStatus.buffers), buf));
            buf.flip();
            dtc.afterRead(true, buf);
            assertFalse(dtc.encodingOrDecoding);
            assertNull(dtc.getPacket());
        });
        assertTrue(deferred.get(10, TimeUnit.SECONDS));
        runInIoThread(() -> {
            assertTrue(dtc.isClosed());
            assertNull(dtc.peer.dtChannel);
            assertFalse(dtc.getChannel().isOpen());
            assertEquals(0, dtc.workerStatus.pendingReqSize());
        });
    }

    @Test
    public void testCloseFromEncoder() throws Exception {
        setup(() -> { }, 1000000);
        DtChannelImpl dtc = client.getPeers().get(0).dtChannel;
        CompletableFuture<Boolean> closedInEncoder = new CompletableFuture<>();
        ByteBufferWritePacket req = new ByteBufferWritePacket(ByteBuffer.allocate(1)) {
            @Override
            protected boolean encodeBody(EncodeContext context, ByteBuffer dest) {
                dtc.peer.shouldAutoReconnect = false;
                client.worker.close(dtc);
                closedInEncoder.complete(dtc.isClosed());
                return super.encodeBody(context, dest);
            }
        };
        req.command = Commands.CMD_PING;
        CompletableFuture<ReadPacket<RefBuffer>> result = new CompletableFuture<>();
        client.sendRequest(req, ctx -> new RefBufferDecoderCallback(),
                new DtTime(10, TimeUnit.SECONDS), RpcCallback.fromFuture(result));
        assertFalse(closedInEncoder.get(10, TimeUnit.SECONDS));
        assertInstanceOf(NetException.class,
                assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS)).getCause());
        runInIoThread(() -> {
            assertFalse(dtc.getChannel().isOpen());
            assertTrue(req.cleaned);
            assertEquals(0, dtc.workerStatus.packetsToWrite);
            assertEquals(0, dtc.workerStatus.pendingReqSize());
            assertEquals(0, client.nioStatus.outPendingRequests);
            assertEquals(0, client.nioStatus.outPendingBytes);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    public void testCascadingCloseFromPendingCallback(int cleanMode) throws Exception {
        setup(() -> { }, 1000000);
        NioServerConfig config = new NioServerConfig();
        config.port = 9001;
        NioServer otherServer = new NioServer(config);
        otherServer.start();
        try {
            Peer otherPeer = client.addPeer(new HostPort("127.0.0.1", 9001)).get(10, TimeUnit.SECONDS);
            client.connect(otherPeer).get(10, TimeUnit.SECONDS);
            runInIoThread(() -> {
                DtChannelImpl first = client.getPeers().get(0).dtChannel;
                DtChannelImpl second = otherPeer.dtChannel;
                first.peer.shouldAutoReconnect = false;
                second.peer.shouldAutoReconnect = false;
                WorkerStatus ws = client.worker.workerStatus;
                ByteBufferWritePacket trigger = new ByteBufferWritePacket(null);
                trigger.packetType = PacketType.TYPE_REQ;
                trigger.seq = first.getAndIncSeq();
                ws.addPendingReq(new PacketInfoReq(null, null, first, trigger, new DtTime(0, TimeUnit.SECONDS),
                        (resp, ex) -> client.worker.close(second), null));
                ByteBufferWritePacket pending = new ByteBufferWritePacket(null);
                pending.packetType = PacketType.TYPE_REQ;
                pending.seq = second.getAndIncSeq();
                CompletableFuture<ReadPacket<Void>> pendingResult = new CompletableFuture<>();
                ws.addPendingReq(new PacketInfoReq(null, null, second, pending, new DtTime(10, TimeUnit.SECONDS),
                        RpcCallback.fromFuture(pendingResult), null));
                ByteBufferWritePacket queued = new ByteBufferWritePacket(null);
                queued.packetType = PacketType.TYPE_REQ;
                CompletableFuture<ReadPacket<Void>> queuedResult = new CompletableFuture<>();
                second.subQueue.enqueue(new PacketInfoReq(null, null, second, queued, new DtTime(10, TimeUnit.SECONDS),
                        RpcCallback.fromFuture(queuedResult), null));
                if (cleanMode == 0) {
                    client.worker.close(first);
                } else if (cleanMode == 1) {
                    ws.ts.refresh();
                    ws.cleanPendingReqByTimeout();
                } else {
                    ws.cleanAllPendingReq();
                }
                assertTrue(pendingResult.isCompletedExceptionally());
                assertTrue(queuedResult.isCompletedExceptionally());
                assertTrue(queued.cleaned);
                assertFalse(second.getChannel().isOpen());
                assertEquals(0, ws.pendingReqSize());
                assertEquals(0, ws.packetsToWrite);
            });
        } finally {
            TestUtil.stop(otherServer);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPeerOperationFromDecoder(boolean remove) throws Exception {
        setup(() -> { }, 1000000);
        Peer peer = client.getPeers().get(0);
        DtChannelImpl dtc = peer.dtChannel;
        CompletableFuture<Boolean> completedAfterClose = new CompletableFuture<>();
        ByteBufferWritePacket req = new ByteBufferWritePacket(ByteBuffer.allocate(1));
        req.command = Commands.CMD_PING;
        client.sendRequest(req, ctx -> {
            CompletableFuture<Void> f = remove ? client.removePeer(peer.endPoint) : client.disconnect(peer);
            boolean completedInDecoder = f.isDone();
            boolean replaced = !remove || client.addPeer(peer.endPoint).getNow(peer) != peer;
            f.thenApply(v -> !completedInDecoder && replaced && !dtc.getChannel().isOpen() && peer.dtChannel == null)
                    .whenComplete((v, ex) -> {
                        if (ex != null) {
                            completedAfterClose.completeExceptionally(ex);
                        } else {
                            completedAfterClose.complete(v);
                        }
                    });
            return new RefBufferDecoderCallback();
        }, new DtTime(10, TimeUnit.SECONDS), (resp, ex) -> {
            if (resp != null) {
                resp.clean();
            }
        });
        assertTrue(completedAfterClose.get(10, TimeUnit.SECONDS));
        if (remove) {
            runInIoThread(() -> {
                assertEquals(PeerStatus.removed, peer.status);
                assertFalse(client.getPeers().contains(peer));
                assertEquals(0, client.worker.workerStatus.retryConnect);
            });
            assertThrows(ExecutionException.class, () -> client.connect(peer).get(10, TimeUnit.SECONDS));
        } else {
            client.connect(peer).get(10, TimeUnit.SECONDS);
            assertNotSame(dtc, peer.dtChannel);
            NioServerClientTest.invoke(client);
        }
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
