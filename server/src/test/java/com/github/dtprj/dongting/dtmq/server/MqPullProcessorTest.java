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
package com.github.dtprj.dongting.dtmq.server;

import com.github.dtprj.dongting.codec.BytesDecoderCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.codec.PbParser;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtmq.proto.DtMq;
import com.github.dtprj.dongting.net.ByteBufferWritePacket;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.Peer;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.raft.impl.DecodeContextEx;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.server.ServerTestBase;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.raft.test.TestUtil;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.dtprj.dongting.raft.impl.RaftTaskTest.buildTestReqDataDirect;
import static com.github.dtprj.dongting.test.Tick.tick;
import static org.junit.jupiter.api.Assertions.*;

class MqPullProcessorTest extends ServerTestBase {
    @Override
    protected void config(RaftGroupConfig config) {
        config.mqIdxItemsPerFile = 256;
    }

    @Override
    protected StateMachine createStateMachine(RaftGroupConfigEx config) {
        MqServerConfig mqConfig = new MqServerConfig();
        mqConfig.maxPullLimit = 1;
        mqConfig.maxPullBytesLimit = 100;
        return new DtMQ(config, mqConfig);
    }

    @Test
    void testPullRpc() throws Exception {
        startAfterCreate = false;
        ServerInfo server = createServer(1, replicateServers(1), "1", "");
        NioClient client = new NioClient(new NioClientConfig());
        try {
            MqServerUtil.initMqServer(server.raftServer);
            server.raftServer.start();
            waitStart(server);
            client.start();
            Peer peer = client.addPeer(new HostPort("127.0.0.1", servicePort(1)))
                    .get(tick(3000), TimeUnit.MILLISECONDS);
            DtMQ mq = (DtMQ) server.group.getStateMachine();

            int bodySize = RaftServerConfig.GATHERING_WRITE_THRESHOLD;
            append(server, 11, bodySize);
            append(server, 11, 32);
            DtMq.MqPullResp first = response(send(client, peer, request(11, 0, 0)));
            assertEquals(1, first.getResults(0).getCount());
            assertEquals(0, first.getResults(0).getFromSeq());
            assertEquals(LogHeader.computeTotalLen(0, bodySize), first.getRecords().size());

            long cursor = first.getResults(0).getFromSeq() + first.getResults(0).getCount();
            DtMq.MqPullResp second = response(send(client, peer,
                    request(11, cursor, 0).toBuilder().setBytesLimit(1).build()));
            assertEquals(1, second.getResults(0).getCount());
            assertEquals(1, second.getResults(0).getFromSeq());
            assertEquals(LogHeader.computeTotalLen(0, 32), second.getRecords().size());

            CompletableFuture<ReadPacket<byte[]>> waiting = send(client, peer, request(22, 0, tick(20_000)));
            WaitUtil.waitUtil(() -> mq.waitingPulls.get(22) != null, server.gc.fiberGroup.getExecutor());
            assertFalse(waiting.isDone());
            append(server, 22, 32);
            assertEquals(1, response(waiting).getResults(0).getCount());
            assertEquals(0, response(send(client, peer, request(22, 1, tick(10)))).getResults(0).getCount());

            assertError(send(client, peer, request(11, 0, 0).toBuilder().setLimit(0).build()), CmdCodes.CLIENT_ERROR);
            assertError(send(client, peer, request(11, 0, 0).toBuilder().setGroupId(-1).build()), CmdCodes.CLIENT_ERROR);
            assertError(send(client, peer, request(11, 0, 0).toBuilder().addQueues(
                    DtMq.MqPullQueue.newBuilder().setQueueId(11)).setQueuesSize(2).build()), CmdCodes.CLIENT_ERROR);

            send(client, peer, request(22, 1, tick(20_000)));
            WaitUtil.waitUtil(() -> mq.waitingPulls.get(22) != null, server.gc.fiberGroup.getExecutor());
            client.disconnect(peer).get(tick(3000), TimeUnit.MILLISECONDS);
            // channel close is checked lazily: data arrival wakes the parked pull and cancels it
            append(server, 22, 32);
            WaitUtil.waitUtil(() -> mq.activePulls.isEmpty() && mq.waitingPulls.size() == 0,
                    server.gc.fiberGroup.getExecutor());
        } finally {
            TestUtil.stop(client);
            waitStop(server);
        }
    }

    @Test
    void testRequestDecoder() {
        DtMq.MqPullReq req = request(11, 9, 123).toBuilder().setQueuesSize(2)
                .addQueues(DtMq.MqPullQueue.newBuilder().setQueueId(22)).build();
        DecodeContextEx context = new DecodeContextEx(null, null);
        for (int chunk : new int[]{1, req.getSerializedSize()}) {
            MqPullReq decoded = decode(context, req.toByteArray(), chunk);
            assertEquals(1, decoded.groupId);
            assertEquals(Integer.MAX_VALUE, decoded.limit);
            assertEquals(Integer.MAX_VALUE, decoded.bytesLimit);
            assertEquals(123, decoded.waitMillis);
            assertArrayEquals(new long[]{11, 22}, decoded.queueIds);
            assertArrayEquals(new long[]{9, 0}, decoded.fromSeqs);
        }
        assertArrayEquals(new long[]{11, 22}, decode(context,
                req.toBuilder().clearQueuesSize().build().toByteArray(), 1).queueIds);
        assertThrows(PbException.class, () -> decode(context,
                req.toBuilder().setQueuesSize(Integer.MAX_VALUE).build().toByteArray(), 1));
        assertThrows(PbException.class, () -> decode(context,
                req.toBuilder().setQueuesSize(1).build().toByteArray(), 1));
        assertEquals(0, decode(context, request(11, 0, 0).toByteArray(), 1).waitMillis);
    }

    private MqPullReq decode(DecodeContextEx context, byte[] bytes, int chunk) {
        PbParser parser = new PbParser();
        parser.prepareNext(context, context.mqPullReqCallback(), bytes.length);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        MqPullReq result = null;
        try {
            while (buf.position() < bytes.length) {
                buf.limit(Math.min(buf.position() + chunk, bytes.length));
                result = (MqPullReq) parser.parse(buf);
            }
            assertTrue(parser.isFinished());
            return result;
        } finally {
            context.reset(parser);
        }
    }

    private DtMq.MqPullReq request(long queueId, long seq, long waitMillis) {
        return DtMq.MqPullReq.newBuilder().setGroupId(1).setQueuesSize(1)
                .addQueues(DtMq.MqPullQueue.newBuilder().setQueueId(queueId).setFromSeq(seq))
                .setLimit(Integer.MAX_VALUE).setBytesLimit(Integer.MAX_VALUE).setWaitMillis(waitMillis).build();
    }

    private CompletableFuture<ReadPacket<byte[]>> send(NioClient client, Peer peer, DtMq.MqPullReq req) {
        ByteBufferWritePacket packet = new ByteBufferWritePacket(ByteBuffer.wrap(req.toByteArray()));
        packet.command = Commands.DTMQ_PULL;
        CompletableFuture<ReadPacket<byte[]>> result = new CompletableFuture<>();
        client.sendRequest(peer, packet, ctx -> new BytesDecoderCallback(),
                new DtTime(tick(30_000), TimeUnit.MILLISECONDS), RpcCallback.fromFuture(result));
        return result;
    }

    private DtMq.MqPullResp response(CompletableFuture<ReadPacket<byte[]>> future) throws Exception {
        return DtMq.MqPullResp.parseFrom(future.get(tick(3000), TimeUnit.MILLISECONDS).getBody());
    }

    private void assertError(CompletableFuture<ReadPacket<byte[]>> future, int code) {
        ExecutionException ex = assertThrows(ExecutionException.class,
                () -> future.get(tick(3000), TimeUnit.MILLISECONDS));
        assertEquals(code, assertInstanceOf(NetCodeException.class, ex.getCause()).getCode());
    }

    private void append(ServerInfo server, long queueId, int bodySize) throws Exception {
        RaftReqData data = buildTestReqDataDirect(LogHeader.TYPE_NORMAL, 1, null, new byte[bodySize]);
        data.bizKey = queueId;
        CompletableFuture<Void> done = new CompletableFuture<>();
        server.group.submitLinearTask(RaftInput.create(data, null, null,
                new DtTime(tick(3000), TimeUnit.MILLISECONDS), false, new RaftCallback() {
                    @Override
                    public void success(long raftIndex, Object result) {
                        done.complete(null);
                    }

                    @Override
                    public void fail(Throwable ex) {
                        done.completeExceptionally(ex);
                    }
                }));
        done.get(tick(3000), TimeUnit.MILLISECONDS);
    }
}
