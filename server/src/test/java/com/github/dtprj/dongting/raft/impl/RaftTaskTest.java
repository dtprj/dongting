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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.raft.store.RaftLogDataCallback;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

import static com.github.dtprj.dongting.raft.impl.RaftUtil.updateCrc;
import static org.junit.jupiter.api.Assertions.*;

public class RaftTaskTest {

    @Test
    void testFullBufferEncodeHeaderOnly() {
        RaftTask task = createTask(0, 0);
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(task);
        decodeFull(buf, 0, 0);
    }

    @Test
    void testFullBufferEncodeWithBizHeader() {
        RaftTask task = createTask(10, 0);
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(task);
        decodeFull(buf, 10, 0);
    }

    @Test
    void testFullBufferEncodeWithBizBody() {
        RaftTask task = createTask(0, 20);
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(task);
        decodeFull(buf, 0, 20);
    }

    @Test
    void testFullBufferEncodeWithBizHeaderAndBody() {
        RaftTask task = createTask(10, 20);
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(task);
        decodeFull(buf, 10, 20);
    }

    @Test
    void testSmallBufferEncodeHeaderOnly() {
        RaftTask task = createTask(0, 0);
        List<RaftReqData> results = new ArrayList<>();
        RaftLogDataCallback callback = new RaftLogDataCallback(results::add);
        CodecTestUtil.smallBufferEncodeAndDecode(task, callback);
        assertEquals(1, results.size());
        assertData(results.get(0), 0, 0);
        results.get(0).release();
    }

    @Test
    void testSmallBufferEncodeWithBizHeader() {
        RaftTask task = createTask(10, 0);
        List<RaftReqData> results = new ArrayList<>();
        RaftLogDataCallback callback = new RaftLogDataCallback(results::add);
        CodecTestUtil.smallBufferEncodeAndDecode(task, callback);
        assertEquals(1, results.size());
        assertData(results.get(0), 10, 0);
        results.get(0).release();
    }

    @Test
    void testSmallBufferEncodeWithBizBody() {
        RaftTask task = createTask(0, 20);
        List<RaftReqData> results = new ArrayList<>();
        RaftLogDataCallback callback = new RaftLogDataCallback(results::add);
        CodecTestUtil.smallBufferEncodeAndDecode(task, callback);
        assertEquals(1, results.size());
        assertData(results.get(0), 0, 20);
        results.get(0).release();
    }

    @Test
    void testSmallBufferEncodeWithBizHeaderAndBody() {
        RaftTask task = createTask(10, 20);
        List<RaftReqData> results = new ArrayList<>();
        RaftLogDataCallback callback = new RaftLogDataCallback(results::add);
        CodecTestUtil.smallBufferEncodeAndDecode(task, callback);
        assertEquals(1, results.size());
        assertData(results.get(0), 10, 20);
        results.get(0).release();
    }

    private void decodeFull(ByteBuffer buf, int expectedHeaderLen, int expectedBodyLen) {
        buf.position(0);
        List<RaftReqData> results = new ArrayList<>();
        RaftLogDataCallback callback = new RaftLogDataCallback(results::add);
        Decoder decoder = new Decoder();
        decoder.prepareNext(CodecTestUtil.decodeContext(), callback);
        decoder.decode(buf, buf.remaining(), 0);
        assertTrue(decoder.isFinished());
        assertEquals(1, results.size());
        assertData(results.get(0), expectedHeaderLen, expectedBodyLen);
        results.get(0).release();
    }

    public static RaftTask createTask(int bizHeaderLen, int bodyLen) {
        byte[] headerBytes = new byte[bizHeaderLen];
        for (int i = 0; i < bizHeaderLen; i++) {
            headerBytes[i] = (byte) i;
        }
        byte[] bodyBytes = new byte[bodyLen];
        for (int i = 0; i < bodyLen; i++) {
            bodyBytes[i] = (byte) i;
        }

        RaftReqData reqData = buildTestReqData(LogHeader.TYPE_NORMAL, 5, headerBytes, bodyBytes);
        return new RaftTask(reqData, null, null, false);
    }

    public static RaftReqData buildTestReqData(int type, int bizType, byte[] bizHeader, byte[] bizBody) {
        int bizHeaderLen = (bizHeader == null) ? 0 : bizHeader.length;
        int bodyLen = (bizBody == null) ? 0 : bizBody.length;
        int totalLen = LogHeader.computeTotalLen(bizHeaderLen, bodyLen);

        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        buf.position(LogHeader.ITEM_HEADER_SIZE);
        if (bizHeaderLen > 0) {
            buf.put(bizHeader);
            CRC32C crc = new CRC32C();
            updateCrc(crc, buf, LogHeader.ITEM_HEADER_SIZE, bizHeaderLen);
            buf.putInt((int) crc.getValue());
        }
        if (bodyLen > 0) {
            int bodyStart = buf.position();
            buf.put(bizBody);
            CRC32C crc = new CRC32C();
            updateCrc(crc, buf, bodyStart, bodyLen);
            buf.putInt((int) crc.getValue());
        }
        buf.flip();
        RefBuffer refBuffer = RefBuffer.wrap(buf);
        refBuffer.prepareForEncode();
        RaftReqData data = new RaftReqData(refBuffer);
        data.type = type;
        data.bizType = bizType;
        data.bizHeaderLen = bizHeaderLen;
        data.bodyLen = bodyLen;
        data.term = 1;
        data.prevLogTerm = 0;
        data.index = 100;
        data.timestamp = System.currentTimeMillis();
        CRC32C crc = new CRC32C();
        LogHeader.writeAndComputeCrc(data, crc, buf);
        buf.position(0);
        buf.limit(totalLen);
        return data;
    }

    public static void assertData(RaftReqData data, int expectedHeaderLen, int expectedBodyLen) {
        assertNotNull(data);
        assertEquals(LogHeader.TYPE_NORMAL, data.type);
        assertEquals(5, data.bizType);
        assertEquals(1, data.term);
        assertEquals(0, data.prevLogTerm);
        assertEquals(100, data.index);
        assertEquals(LogHeader.computeTotalLen(expectedHeaderLen, expectedBodyLen), data.totalLen);

        if (expectedHeaderLen > 0) {
            ByteBuffer hb = data.prepareReadBizHeader();
            assertNotNull(hb);
            assertEquals(expectedHeaderLen, hb.remaining());
            int pos = hb.position();
            for (int i = 0; i < expectedHeaderLen; i++) {
                assertEquals((byte) i, hb.get(pos + i));
            }
        } else {
            assertNull(data.prepareReadBizHeader());
        }

        if (expectedBodyLen > 0) {
            ByteBuffer bb = data.prepareReadBizBody();
            assertNotNull(bb);
            assertEquals(expectedBodyLen, bb.remaining());
            int pos = bb.position();
            for (int i = 0; i < expectedBodyLen; i++) {
                assertEquals((byte) i, bb.get(pos + i));
            }
        } else {
            assertNull(data.prepareReadBizBody());
        }
        data.reset();
    }
}
