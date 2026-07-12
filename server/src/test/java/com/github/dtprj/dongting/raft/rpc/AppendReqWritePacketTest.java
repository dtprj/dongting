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
package com.github.dtprj.dongting.raft.rpc;

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbParser;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftTaskTest;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class AppendReqWritePacketTest {

    private final RaftCodecFactory raftCodecFactory = new RaftCodecFactory() {

        @Override
        public DecoderCallback<? extends Encodable> createBodyCallback(int bizType, DecodeContext context) {
            return new ByteArray.Callback();
        }

        @Override
        public DecoderCallback<? extends Encodable> createHeaderCallback(int bizType, DecodeContext context) {
            return new ByteArray.Callback();
        }
    };

    @Test
    public void testEncode() {
        testEncode0(true, true);
        testEncode0(false, false);
    }

    private void testEncode0(boolean addHeader, boolean addBody) {
        AppendReqWritePacket f = createFrame(addHeader, addBody);
        ByteBuffer buf = ByteBuffer.allocate(f.actualBodySize());
        Assertions.assertTrue(f.encodeBody(new EncodeContext(null), buf));
        assertEquals(buf.position(), f.actualBodySize());

        buf.clear();

        DecodeContext decodeContext = CodecTestUtil.decodeContext();
        AppendReq.Callback c = new AppendReq.Callback(g -> raftCodecFactory, decodeContext.buffers, decodeContext.threadLocalBuffer);
        PbParser p = new PbParser();
        p.prepareNext(decodeContext, c, f.actualBodySize());
        AppendReq result = (AppendReq) p.parse(buf);

        check(f, result);
    }

    @Test
    public void testSmallBufferEncode() {
        for (int i = 0; i < 100; i++) {
            testSmallBufferEncode0(true, true);
            testSmallBufferEncode0(false, false);
        }
    }

    private void testSmallBufferEncode0(boolean addHeader, boolean addBody) {
        AppendReqWritePacket f = createFrame(addHeader, addBody);
        EncodeContext context = new EncodeContext(null);
        DecodeContext decodeContext = CodecTestUtil.decodeContext();
        AppendReq.Callback c = new AppendReq.Callback(g -> raftCodecFactory, decodeContext.buffers, decodeContext.threadLocalBuffer);
        PbParser p = new PbParser();
        p.prepareNext(decodeContext, c, f.actualBodySize());
        Random r = new Random();
        int actualBodySize = f.actualBodySize();
        AppendReq parseResult = null;
        for (int encodeBytes = 0; encodeBytes < actualBodySize; ) {
            int size = r.nextBoolean() ? r.nextInt(5) + 1 : r.nextInt(100) + 1;
            ByteBuffer buf = ByteBuffer.allocate(size);
            boolean result = f.encodeBody(context, buf);
            if (encodeBytes + buf.capacity() >= actualBodySize) {
                assertTrue(result);
                assertEquals(actualBodySize, encodeBytes + buf.position());
            }
            buf.flip();
            encodeBytes += buf.remaining();
            if (buf.remaining() > 0) {
                parseResult = (AppendReq) p.parse(buf);
            }
        }
        check(f, parseResult);
    }

    // ---- pre-encoded (direct buffer) zero-copy tests ----

    @Test
    public void testEncode_DirectBuffer() {
        testEncodeWithPreEncoded0(true, true);
        testEncodeWithPreEncoded0(false, false);
    }

    @Test
    public void testEncode_MixedDirectAndHeap() {
        testEncodeWithPreEncodedMixed(true, true);
        testEncodeWithPreEncodedMixed(false, false);
    }

    private void testEncodeWithPreEncoded0(boolean addHeader, boolean addBody) {
        AppendReqWritePacket f = createFrame(addHeader, addBody, true);
        f.actualBodySize(); // trigger calcActualBodySize which computes totalPreEncodedSize
        assertTrue(f.getTotalPreEncodedBufferSize() > 0);

        ByteBuffer body = encodeBodyCollectPreEncoded(f, f.actualBodySize());

        DecodeContext decodeContext = CodecTestUtil.decodeContext();
        AppendReq.Callback c = new AppendReq.Callback(g -> raftCodecFactory,
                decodeContext.buffers, decodeContext.threadLocalBuffer);
        PbParser p = new PbParser();
        p.prepareNext(decodeContext, c, f.actualBodySize());
        AppendReq result = (AppendReq) p.parse(body);
        check(f, result);
    }

    private void testEncodeWithPreEncodedMixed(boolean addHeader, boolean addBody) {
        AppendReqWritePacket f = createMixedFrame(addHeader, addBody);
        f.actualBodySize(); // trigger calcActualBodySize which computes totalPreEncodedSize
        assertTrue(f.getTotalPreEncodedBufferSize() > 0);

        ByteBuffer body = encodeBodyCollectPreEncoded(f, f.actualBodySize());

        DecodeContext decodeContext = CodecTestUtil.decodeContext();
        AppendReq.Callback c = new AppendReq.Callback(g -> raftCodecFactory,
                decodeContext.buffers, decodeContext.threadLocalBuffer);
        PbParser p = new PbParser();
        p.prepareNext(decodeContext, c, f.actualBodySize());
        AppendReq result = (AppendReq) p.parse(body);
        check(f, result);
    }

    @Test
    public void testSmallBufferEncode_DirectBuffer() {
        for (int i = 0; i < 100; i++) {
            testSmallBufferEncodeWithPreEncoded0(true, true);
        }
    }

    @Test
    public void testSmallBufferEncode_MixedDirectAndHeap() {
        for (int i = 0; i < 100; i++) {
            testSmallBufferEncodeWithPreEncodedMixed(true, true);
        }
    }

    private void testSmallBufferEncodeWithPreEncoded0(boolean addHeader, boolean addBody) {
        AppendReqWritePacket f = createFrame(addHeader, addBody, true);
        smallBufferEncodeAndCheck(f);
    }

    private void testSmallBufferEncodeWithPreEncodedMixed(boolean addHeader, boolean addBody) {
        AppendReqWritePacket f = createMixedFrame(addHeader, addBody);
        smallBufferEncodeAndCheck(f);
    }

    private void smallBufferEncodeAndCheck(AppendReqWritePacket f) {
        EncodeContext context = new EncodeContext(null);
        int actualBodySize = f.actualBodySize();
        DecodeContext decodeContext = CodecTestUtil.decodeContext();
        AppendReq.Callback c = new AppendReq.Callback(g -> raftCodecFactory,
                decodeContext.buffers, decodeContext.threadLocalBuffer);
        PbParser p = new PbParser();
        p.prepareNext(decodeContext, c, actualBodySize);
        Random r = new Random();

        int totalInline = 0;
        AppendReq parseResult = null;
        while (true) {
            int size = r.nextBoolean() ? r.nextInt(5) + 1 : r.nextInt(100) + 1;
            ByteBuffer buf = ByteBuffer.allocate(size);
            boolean finished = f.encodeBody(context, buf);
            buf.flip();
            if (buf.remaining() > 0) {
                totalInline += buf.remaining();
                parseResult = (AppendReq) p.parse(buf);
            }
            if (finished) {
                break;
            }
            if (f.hasPreEncodedBuffer()) {
                ByteBuffer preBuf = f.getPreEncodedBuffer();
                assertNotNull(preBuf);
                assertTrue(preBuf.remaining() > 0);
                parseResult = (AppendReq) p.parse(preBuf);
            }
        }
        assertEquals(actualBodySize, totalInline + f.getTotalPreEncodedBufferSize());
        check(f, parseResult);
    }

    private ByteBuffer encodeBodyCollectPreEncoded(AppendReqWritePacket f, int bufCapacity) {
        EncodeContext context = new EncodeContext(null);
        int actualBodySize = f.actualBodySize();
        ByteBuffer dest = ByteBuffer.allocate(bufCapacity);
        ByteBuffer result = ByteBuffer.allocate(actualBodySize);
        int lastFlushPos = 0;

        while (true) {
            boolean finished = f.encodeBody(context, dest);
            int currentPos = dest.position();
            if (currentPos > lastFlushPos) {
                dest.position(lastFlushPos);
                dest.limit(currentPos);
                result.put(dest);
                dest.limit(dest.capacity());
                dest.position(currentPos);
            }
            lastFlushPos = currentPos;
            if (finished) {
                break;
            }
            assertTrue(f.hasPreEncodedBuffer(),
                    "encodeBody returned false but no pre-encoded buffer");
            ByteBuffer preBuf = f.getPreEncodedBuffer();
            assertNotNull(preBuf);
            assertTrue(preBuf.remaining() > 0);
            result.put(preBuf);
        }
        result.flip();
        assertEquals(actualBodySize, result.remaining());
        return result;
    }

    private AppendReqWritePacket createFrame(boolean addHeader, boolean addBody) {
        return createFrame(addHeader, addBody, false);
    }

    private AppendReqWritePacket createFrame(boolean addHeader, boolean addBody, boolean useDirect) {
        AppendReqWritePacket f = new AppendReqWritePacket();
        f.groupId = 12345;
        f.term = 4;
        f.leaderId = 2;
        f.prevLogIndex = 100;
        f.prevLogTerm = 3;
        f.leaderCommit = 99;
        ArrayList<RaftTask> logs = new ArrayList<>();
        f.logs = logs;
        for (int i = 0; i < 2; i++) {
            byte[] bizHeader = addHeader ? new byte[10] : null;
            if (bizHeader != null) {
                new Random().nextBytes(bizHeader);
            }
            byte[] bizBody = useDirect
                    ? new byte[RaftServerConfig.GATHERING_WRITE_THRESHOLD]
                    : (addBody ? new byte[20] : null);
            if (bizBody != null) {
                new Random().nextBytes(bizBody);
            }
            RaftReqData reqData = useDirect
                    ? RaftTaskTest.buildTestReqDataDirect(LogHeader.TYPE_NORMAL, 1, bizHeader, bizBody)
                    : RaftTaskTest.buildTestReqData(LogHeader.TYPE_NORMAL, 1, bizHeader, bizBody);

            reqData.term = 4;
            reqData.prevLogTerm = 3;
            reqData.index = 200 + i;
            reqData.timestamp = System.currentTimeMillis();
            LogHeader.writeAndComputeCrc(reqData, new java.util.zip.CRC32C(), reqData.buffer.getBuffer(), 0);

            RaftTask rt = new RaftTask(reqData, null, null, false);
            logs.add(rt);
        }
        return f;
    }

    private AppendReqWritePacket createMixedFrame(boolean addHeader, boolean addBody) {
        AppendReqWritePacket f = new AppendReqWritePacket();
        f.groupId = 12345;
        f.term = 4;
        f.leaderId = 2;
        f.prevLogIndex = 100;
        f.prevLogTerm = 3;
        f.leaderCommit = 99;
        ArrayList<RaftTask> logs = new ArrayList<>();
        f.logs = logs;
        addLog(logs, addHeader, addBody, 200, false);
        addLog(logs, addHeader, addBody, 201, true);
        addLog(logs, addHeader, addBody, 202, true);
        addLog(logs, addHeader, addBody, 203, false);
        return f;
    }

    private void addLog(List<RaftTask> logs, boolean addHeader, boolean addBody, int index, boolean useDirect) {
        byte[] bizHeader = addHeader ? new byte[10] : null;
        if (bizHeader != null) {
            new Random().nextBytes(bizHeader);
        }
        byte[] bizBody = useDirect
                ? new byte[RaftServerConfig.GATHERING_WRITE_THRESHOLD]
                : (addBody ? new byte[20] : null);
        if (bizBody != null) {
            new Random().nextBytes(bizBody);
        }
        RaftReqData reqData = useDirect
                ? RaftTaskTest.buildTestReqDataDirect(LogHeader.TYPE_NORMAL, 1, bizHeader, bizBody)
                : RaftTaskTest.buildTestReqData(LogHeader.TYPE_NORMAL, 1, bizHeader, bizBody);
        reqData.term = 4;
        reqData.prevLogTerm = 3;
        reqData.index = index;
        reqData.timestamp = System.currentTimeMillis();
        LogHeader.writeAndComputeCrc(reqData, new java.util.zip.CRC32C(), reqData.buffer.getBuffer(), 0);
        logs.add(new RaftTask(reqData, null, null, false));
    }

    private void check(AppendReqWritePacket f, AppendReq c) {
        assertEquals(f.groupId, c.groupId);
        assertEquals(f.term, c.term);
        assertEquals(f.leaderId, c.leaderId);
        assertEquals(f.prevLogIndex, c.prevLogIndex);
        assertEquals(f.prevLogTerm, c.prevLogTerm);
        assertEquals(f.leaderCommit, c.leaderCommit);
        assertEquals(f.logs.size(), c.logs.size());
        for (int i = 0; i < f.logs.size(); i++) {
            RaftTask l1 = f.logs.get(i);
            RaftTask l2 = c.logs.get(i);
            assertEquals(l1.reqData.bizType, l2.reqData.bizType);
            assertEquals(l1.reqData.index, l2.reqData.index);
            assertEquals(l1.reqData.term, l2.reqData.term);
            assertEquals(l1.reqData.timestamp, l2.reqData.timestamp);
            assertEquals(l1.reqData.type, l2.reqData.type);
            ByteBuffer h1 = l1.reqData.prepareReadBizHeader();
            ByteBuffer h2 = l2.reqData.prepareReadBizHeader();
            if (h1 != null) {
                assertNotNull(h2);
                assertEquals(h1.remaining(), h2.remaining());
                int pos1 = h1.position();
                int pos2 = h2.position();
                for (int j = 0; j < h1.remaining(); j++) {
                    assertEquals(h1.get(pos1 + j), h2.get(pos2 + j));
                }
            } else {
                assertNull(h2);
            }
            ByteBuffer b1 = l1.reqData.prepareReadBizBody();
            ByteBuffer b2 = l2.reqData.prepareReadBizBody();
            if (b1 != null) {
                assertNotNull(b2);
                assertEquals(b1.remaining(), b2.remaining());
                int pos1 = b1.position();
                int pos2 = b2.position();
                for (int j = 0; j < b1.remaining(); j++) {
                    assertEquals(b1.get(pos1 + j), b2.get(pos2 + j));
                }
            } else {
                assertNull(b2);
            }
            l1.reqData.reset();
            l2.reqData.reset();
        }
    }
}
