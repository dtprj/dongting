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

import com.github.dtprj.dongting.buf.Buffers;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.buf.SimpleByteBufferPoolConfig;
import com.github.dtprj.dongting.codec.BytesDecoderCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.codec.PbParser;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtmq.MqClientConfig;
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
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.server.ServerTestBase;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.raft.test.TestUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

import static com.github.dtprj.dongting.test.Tick.tick;
import static org.junit.jupiter.api.Assertions.*;

class MqSendProcessorTest extends ServerTestBase {
    private static final int MAX_BODY = RaftServerConfig.GATHERING_WRITE_THRESHOLD;
    private static final ByteString HEADER = ByteString.copyFrom(new byte[]{7, 0, -1, 3});

    @Override
    protected void config(RaftGroupConfig config) {
        config.mqIdxItemsPerFile = 256;
    }

    @Override
    protected StateMachine createStateMachine(RaftGroupConfigEx config) {
        MqServerConfig mqConfig = new MqServerConfig();
        mqConfig.maxBody = MAX_BODY;
        return new DtMQ(config, mqConfig);
    }

    @Test
    void testSendRpcAndBatchValidation() throws Exception {
        MqClientConfig defaults = new MqServerConfig();
        assertEquals(2_000_000, defaults.maxBody);
        startAfterCreate = false;
        ServerInfo server = createServer(1, replicateServers(1), "1", "");
        NioClient client = new NioClient(new NioClientConfig());
        try {
            MqServerUtil.initMqServer(server.raftServer);
            server.raftServer.start();
            server.raftServer.getAllGroupReadyFuture().get(tick(5000), TimeUnit.MILLISECONDS);
            client.start();
            Peer peer = client.addPeer(new HostPort("127.0.0.1", servicePort(1)))
                    .get(tick(3000), TimeUnit.MILLISECONDS);

            DtMq.MqMessage single = message(11, ByteString.EMPTY, body(32));
            DtMq.MqSendResp first = send(client, peer, request(single));
            assertEquals(1, first.getRaftIndexesCount());
            assertTrue(first.getRaftIndexes(0) > 0);

            DtMq.MqMessage[] batch = {
                    message(22, HEADER, body(MAX_BODY)),
                    message(11, ByteString.EMPTY, body(MAX_BODY)),
                    message(22, ByteString.EMPTY, ByteString.EMPTY),
                    message(11, HEADER, ByteString.EMPTY)
            };
            DtMq.MqSendResp sent = send(client, peer, request(batch).toBuilder().clearMessagesSize().build());
            assertEquals(batch.length, sent.getRaftIndexesCount());
            long previous = first.getRaftIndexes(0);
            for (long index : sent.getRaftIndexesList()) {
                assertTrue(index > previous);
                previous = index;
            }
            assertPull(client, peer, 11,
                    new long[]{first.getRaftIndexes(0), sent.getRaftIndexes(1), sent.getRaftIndexes(3)},
                    single, batch[1], batch[3]);
            assertPull(client, peer, 22, new long[]{sent.getRaftIndexes(0), sent.getRaftIndexes(2)},
                    batch[0], batch[2]);

            DtMq.MqMessage valid = message(33, ByteString.EMPTY, body(16));
            assertSendError(client, peer, request(valid, message(44, HEADER, body(MAX_BODY + 1))));
            assertSendError(client, peer, request());
            for (int invalidGroupId : new int[]{0, -1}) {
                assertSendError(client, peer, request(valid).toBuilder().setGroupId(invalidGroupId).build());
            }
            // A successful write also waits for any incorrectly submitted prefix of the rejected batch.
            DtMq.MqMessage marker = message(33, HEADER, ByteString.EMPTY);
            DtMq.MqSendResp afterRejection = send(client, peer, request(marker));
            assertEquals(1, afterRejection.getRaftIndexesCount());
            assertPull(client, peer, 33, new long[]{afterRejection.getRaftIndexes(0)}, marker);
            assertPull(client, peer, 44, new long[0]);
        } finally {
            TestUtil.stop(client);
            waitStop(server);
        }
    }

    @Test
    void testRequestDecoder() throws Exception {
        DtMq.MqSendReq req = request(
                message(11, ByteString.EMPTY, body(MAX_BODY)),
                message(22, HEADER, body(MAX_BODY)),
                message(11, HEADER, ByteString.EMPTY),
                message(22, ByteString.EMPTY, ByteString.EMPTY));
        try (TrackingBuffers buffers = new TrackingBuffers()) {
            DecodeContextEx context = new DecodeContextEx(buffers, null);
            for (byte[] bytes : new byte[][]{req.toByteArray(), repeatedFieldsRequest()}) {
                for (int chunk : new int[]{bytes.length, 1}) {
                    assertDecoded(context, bytes, chunk);
                    buffers.assertAllReleased();
                }
            }
        }
    }

    @Test
    void testDecoderResetAndInvalidSize() throws Exception {
        DtMq.MqSendReq req = request(
                message(11, ByteString.EMPTY, body(MAX_BODY)),
                message(22, ByteString.EMPTY, body(MAX_BODY)));
        byte[] bytes = req.toByteArray();
        try (TrackingBuffers buffers = new TrackingBuffers()) {
            DecodeContextEx context = new DecodeContextEx(buffers, null);
            PbParser parser = new PbParser();
            parser.prepareNext(context, context.mqSendReqCallback(), bytes.length);
            try {
                ByteBuffer partial = ByteBuffer.wrap(bytes);
                partial.limit(bytes.length - MAX_BODY / 2);
                assertNull(parser.parse(partial));
                assertFalse(parser.isFinished());
                assertEquals(2, buffers.borrowed.size());
                for (RefBuffer buffer : buffers.borrowed) {
                    assertTrue(buffer.getBuffer().isDirect());
                    assertFalse(buffer.isReleased());
                }
            } finally {
                context.reset(parser);
            }
            buffers.assertAllReleased();
            assertDecoded(context, bytes, 1);
            buffers.assertAllReleased();

            for (int invalidSize : new int[]{Integer.MAX_VALUE, 1}) {
                byte[] invalid = req.toBuilder().setMessagesSize(invalidSize).build().toByteArray();
                assertThrows(PbException.class, () -> decode(context, invalid, 1));
                assertDecoded(context, bytes, bytes.length);
                buffers.assertAllReleased();
            }
        }
    }

    private static DtMq.MqMessage message(long queueId, ByteString header, ByteString body) {
        return DtMq.MqMessage.newBuilder().setQueueId(queueId).setMsgHeader(header).setMsgBody(body).build();
    }

    private static DtMq.MqSendReq request(DtMq.MqMessage... messages) {
        return DtMq.MqSendReq.newBuilder().setGroupId(1).setMessagesSize(messages.length)
                .addAllMessages(Arrays.asList(messages)).build();
    }

    private static ByteString body(int size) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) (i * 31 + 7);
        }
        return ByteString.copyFrom(bytes);
    }

    private static byte[] repeatedFieldsRequest() throws Exception {
        ByteArrayOutputStream message = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(message);
        out.writeBytes(3, body(MAX_BODY));
        out.writeFixed64(1, 11);
        out.writeBytes(2, ByteString.copyFromUtf8("old header"));
        out.writeBytes(3, body(17));
        out.writeBytes(2, HEADER);
        out.flush();

        ByteArrayOutputStream request = new ByteArrayOutputStream();
        out = CodedOutputStream.newInstance(request);
        out.writeInt32(1, 1);
        out.writeByteArray(3, message.toByteArray());
        out.flush();
        return request.toByteArray();
    }

    private MqSendReq decode(DecodeContextEx context, byte[] bytes, int chunk) {
        PbParser parser = new PbParser();
        parser.prepareNext(context, context.mqSendReqCallback(), bytes.length);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        MqSendReq result = null;
        try {
            while (buf.position() < bytes.length) {
                buf.limit(Math.min(buf.position() + chunk, bytes.length));
                result = (MqSendReq) parser.parse(buf);
            }
            assertTrue(parser.isFinished());
            return result;
        } finally {
            context.reset(parser);
        }
    }

    private void assertDecoded(DecodeContextEx context, byte[] bytes, int chunk) throws Exception {
        DtMq.MqSendReq expected = DtMq.MqSendReq.parseFrom(bytes);
        MqSendReq actual = decode(context, bytes, chunk);
        assertNotNull(actual);
        try {
            assertEquals(expected.getGroupId(), actual.groupId);
            assertEquals(expected.getMessagesCount(), actual.messages.size());
            for (int i = 0; i < actual.messages.size(); i++) {
                DtMq.MqMessage message = expected.getMessages(i);
                RaftReqData data = actual.messages.get(i);
                assertEquals(LogHeader.TYPE_NORMAL, data.type);
                assertEquals(message.getQueueId(), data.bizKey);
                assertEquals(message.getMsgHeader().size(), data.bizHeaderLen);
                assertEquals(message.getMsgBody().size(), data.bodyLen);
                assertEquals(LogHeader.computeTotalLen(data.bizHeaderLen, data.bodyLen), data.totalLen);
                ByteBuffer raw = data.buffer.getBuffer().duplicate();
                assertEquals(data.totalLen >= RaftServerConfig.GATHERING_WRITE_THRESHOLD, raw.isDirect());
                assertEquals(data.totalLen, raw.remaining());
                raw.position(LogHeader.ITEM_HEADER_SIZE);
                assertPayload(raw, message.getMsgHeader());
                assertPayload(raw, message.getMsgBody());
                assertFalse(raw.hasRemaining());
            }
        } finally {
            actual.clean();
        }
    }

    private CompletableFuture<ReadPacket<byte[]>> rpc(NioClient client, Peer peer, int command, MessageLite req) {
        ByteBufferWritePacket packet = new ByteBufferWritePacket(ByteBuffer.wrap(req.toByteArray()));
        packet.command = command;
        CompletableFuture<ReadPacket<byte[]>> result = new CompletableFuture<>();
        client.sendRequest(peer, packet, ctx -> new BytesDecoderCallback(),
                new DtTime(tick(5000), TimeUnit.MILLISECONDS), RpcCallback.fromFuture(result));
        return result;
    }

    private DtMq.MqSendResp send(NioClient client, Peer peer, DtMq.MqSendReq req) throws Exception {
        return DtMq.MqSendResp.parseFrom(rpc(client, peer, Commands.DTMQ_SEND, req)
                .get(tick(5000), TimeUnit.MILLISECONDS).getBody());
    }

    private void assertSendError(NioClient client, Peer peer, DtMq.MqSendReq req) {
        ExecutionException ex = assertThrows(ExecutionException.class,
                () -> rpc(client, peer, Commands.DTMQ_SEND, req).get(tick(5000), TimeUnit.MILLISECONDS));
        assertEquals(CmdCodes.CLIENT_ERROR, assertInstanceOf(NetCodeException.class, ex.getCause()).getCode());
    }

    private void assertPull(NioClient client, Peer peer, long queueId, long[] indexes,
                            DtMq.MqMessage... messages) throws Exception {
        DtMq.MqPullReq req = DtMq.MqPullReq.newBuilder().setGroupId(1).setQueuesSize(1)
                .addQueues(DtMq.MqPullQueue.newBuilder().setQueueId(queueId))
                .setLimit(Integer.MAX_VALUE).setBytesLimit(Integer.MAX_VALUE).build();
        DtMq.MqPullResp resp = DtMq.MqPullResp.parseFrom(rpc(client, peer, Commands.DTMQ_PULL, req)
                .get(tick(5000), TimeUnit.MILLISECONDS).getBody());
        assertEquals(1, resp.getResultsSize());
        assertEquals(1, resp.getResultsCount());
        DtMq.MqPullQueueResult result = resp.getResults(0);
        assertEquals(queueId, result.getQueueId());
        assertEquals(0, result.getFromSeq());
        assertEquals(messages.length, result.getCount());
        assertEquals(messages.length, result.getNextSeq());
        assertFalse(result.getTruncated());
        ByteBuffer records = resp.getRecords().asReadOnlyByteBuffer();
        for (int i = 0; i < messages.length; i++) {
            int start = records.position();
            DtMq.MqMessage message = messages[i];
            LogHeader header = new LogHeader();
            assertTrue(header.readAndCheckCrc(new CRC32C(), records));
            assertEquals(LogHeader.TYPE_NORMAL, header.type);
            assertEquals(queueId, header.bizKey);
            assertEquals(indexes[i], header.index);
            assertEquals(message.getMsgHeader().size(), header.bizHeaderLen);
            assertEquals(message.getMsgBody().size(), header.bodyLen);
            assertEquals(LogHeader.computeTotalLen(header.bizHeaderLen, header.bodyLen), header.totalLen);
            assertPayload(records, message.getMsgHeader());
            assertPayload(records, message.getMsgBody());
            assertEquals(start + header.totalLen, records.position());
        }
        assertFalse(records.hasRemaining());
    }

    private static void assertPayload(ByteBuffer buf, ByteString expected) {
        byte[] actual = new byte[expected.size()];
        buf.get(actual);
        assertArrayEquals(expected.toByteArray(), actual);
        if (!expected.isEmpty()) {
            CRC32C crc = new CRC32C();
            crc.update(actual, 0, actual.length);
            assertEquals((int) crc.getValue(), buf.getInt());
        }
    }

    private static final class TrackingBuffers extends Buffers implements AutoCloseable {
        private final SimpleByteBufferPool heap = pool(false);
        private final SimpleByteBufferPool direct = pool(true);
        private final List<RefBuffer> borrowed = new ArrayList<>();

        private static SimpleByteBufferPool pool(boolean direct) {
            return new SimpleByteBufferPool(new SimpleByteBufferPoolConfig(direct, 0,
                    new int[]{MAX_BODY * 2}, new int[]{0}, new int[]{8}));
        }

        @Override
        public RefBuffer borrow(int requestSize, boolean plain, boolean threadSafeRelease, int threshold) {
            RefBuffer buffer = heap.borrow(plain, requestSize);
            borrowed.add(buffer);
            return buffer;
        }

        @Override
        public RefBuffer borrowDirect(int requestSize) {
            RefBuffer buffer = direct.borrow(false, requestSize);
            borrowed.add(buffer);
            return buffer;
        }

        private void assertAllReleased() {
            for (int i = 0; i < borrowed.size(); i++) {
                assertTrue(borrowed.get(i).isReleased(), "unreleased buffer " + i);
            }
        }

        @Override
        public void close() {
            borrowed.forEach(RefBuffer::clean);
            heap.cleanAll();
            direct.cleanAll();
        }
    }
}
