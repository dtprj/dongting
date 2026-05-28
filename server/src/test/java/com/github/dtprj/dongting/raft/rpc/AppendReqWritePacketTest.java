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
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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

    private AppendReqWritePacket createFrame(boolean addHeader, boolean addBody) {
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
            byte[] bizBody = addBody ? new byte[20] : null;
            if (bizBody != null) {
                new Random().nextBytes(bizBody);
            }
            RaftReqData reqData = RaftTaskTest.buildTestReqData(LogHeader.TYPE_NORMAL, 1,
                    bizHeader, bizBody);

            LogHeader lh = reqData.logHeader;
            lh.term = 4;
            lh.prevLogTerm = 3;
            lh.index = 200 + i;
            lh.timestamp = System.currentTimeMillis();
            lh.writeAndComputeCrc(new java.util.zip.CRC32C(), reqData.buffer.getBuffer(), 0);

            RaftTask rt = new RaftTask(reqData, null, null, false);
            logs.add(rt);
        }
        return f;
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
            assertEquals(l1.bizType, l2.bizType);
            assertEquals(l1.logHeader.index, l2.logHeader.index);
            assertEquals(l1.logHeader.term, l2.logHeader.term);
            assertEquals(l1.logHeader.timestamp, l2.logHeader.timestamp);
            assertEquals(l1.logHeader.type, l2.logHeader.type);
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
