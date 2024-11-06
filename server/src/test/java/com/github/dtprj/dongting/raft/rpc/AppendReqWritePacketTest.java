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
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;
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

    private static Encodable createBytes(int size) {
        byte[] bytes = new byte[size];
        new Random().nextBytes(bytes);
        return new ByteArray(bytes);
    }

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
        AppendReq.Callback c = new AppendReq.Callback(g -> raftCodecFactory);
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
        AppendReq.Callback c = new AppendReq.Callback(g -> raftCodecFactory);
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
        f.setGroupId(12345);
        f.setTerm(4);
        f.setLeaderId(2);
        f.setPrevLogIndex(100);
        f.setPrevLogTerm(3);
        f.setLeaderCommit(99);
        f.setLogs(null);
        ArrayList<LogItem> logs = new ArrayList<>();
        f.setLogs(logs);
        for (int i = 0; i < 2; i++) {
            LogItem log = new LogItem();
            log.setBizType(1);
            log.setIndex(200 + i);
            log.setTerm(4);
            log.setTimestamp(System.currentTimeMillis());
            log.setType(LogItem.TYPE_NORMAL);
            if (addHeader) {
                log.setHeader(createBytes(10));
            }
            if (addBody) {
                log.setBody(createBytes(20));
            }
            logs.add(log);
        }
        return f;
    }

    private void check(AppendReqWritePacket f, AppendReq c) {
        assertEquals(f.groupId, c.getGroupId());
        assertEquals(f.term, c.getTerm());
        assertEquals(f.leaderId, c.getLeaderId());
        assertEquals(f.prevLogIndex, c.getPrevLogIndex());
        assertEquals(f.prevLogTerm, c.getPrevLogTerm());
        assertEquals(f.leaderCommit, c.getLeaderCommit());
        assertEquals(f.logs.size(), c.getLogs().size());
        for (int i = 0; i < f.logs.size(); i++) {
            LogItem l1 = f.logs.get(i);
            LogItem l2 = c.getLogs().get(i);
            assertEquals(l1.getBizType(), l2.getBizType());
            assertEquals(l1.getIndex(), l2.getIndex());
            assertEquals(l1.getTerm(), l2.getTerm());
            assertEquals(l1.getTimestamp(), l2.getTimestamp());
            assertEquals(l1.getType(), l2.getType());
            if (l1.getHeader() != null) {
                assertArrayEquals(((ByteArray) l1.getHeader()).getData(),
                        ((ByteArray) l2.getHeader()).getData());
            } else {
                assertNull(l2.getHeader());
            }
            if (l1.getBody() != null) {
                assertArrayEquals(((ByteArray) l1.getBody()).getData(),
                        ((ByteArray) l2.getBody()).getData());
            } else {
                assertNull(l2.getBody());
            }
        }
    }
}
