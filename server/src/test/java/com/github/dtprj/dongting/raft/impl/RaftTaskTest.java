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
import com.github.dtprj.dongting.raft.store.RaftLogData;
import com.github.dtprj.dongting.raft.store.RaftLogDataCallback;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

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
        List<RaftLogData> results = new ArrayList<>();
        RaftLogDataCallback callback = new RaftLogDataCallback(results::add);
        CodecTestUtil.smallBufferEncodeAndDecode(task, callback);
        assertEquals(1, results.size());
        assertData(results.get(0), 0, 0);
        results.get(0).release();
    }

    @Test
    void testSmallBufferEncodeWithBizHeader() {
        RaftTask task = createTask(10, 0);
        List<RaftLogData> results = new ArrayList<>();
        RaftLogDataCallback callback = new RaftLogDataCallback(results::add);
        CodecTestUtil.smallBufferEncodeAndDecode(task, callback);
        assertEquals(1, results.size());
        assertData(results.get(0), 10, 0);
        results.get(0).release();
    }

    @Test
    void testSmallBufferEncodeWithBizBody() {
        RaftTask task = createTask(0, 20);
        List<RaftLogData> results = new ArrayList<>();
        RaftLogDataCallback callback = new RaftLogDataCallback(results::add);
        CodecTestUtil.smallBufferEncodeAndDecode(task, callback);
        assertEquals(1, results.size());
        assertData(results.get(0), 0, 20);
        results.get(0).release();
    }

    @Test
    void testSmallBufferEncodeWithBizHeaderAndBody() {
        RaftTask task = createTask(10, 20);
        List<RaftLogData> results = new ArrayList<>();
        RaftLogDataCallback callback = new RaftLogDataCallback(results::add);
        CodecTestUtil.smallBufferEncodeAndDecode(task, callback);
        assertEquals(1, results.size());
        assertData(results.get(0), 10, 20);
        results.get(0).release();
    }

    private void decodeFull(ByteBuffer buf, int expectedHeaderLen, int expectedBodyLen) {
        buf.position(0);
        List<RaftLogData> results = new ArrayList<>();
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

        RefBuffer bizHeaderBuffer = bizHeaderLen > 0 ? RefBuffer.wrap(ByteBuffer.wrap(headerBytes)) : null;
        int headerCrc = calcCrc(bizHeaderBuffer);
        RefBuffer bizBodyBuffer = bodyLen > 0 ? RefBuffer.wrap(ByteBuffer.wrap(bodyBytes)) : null;
        int bodyCrc = calcCrc(bizBodyBuffer);

        RaftReqData reqData = new RaftReqData(bizHeaderBuffer, headerCrc, bizBodyBuffer, bodyCrc);

        LogHeader lh = new LogHeader();
        lh.type = LogHeader.TYPE_NORMAL;
        lh.term = 1;
        lh.prevLogTerm = 0;
        lh.index = 100;
        lh.timestamp = System.currentTimeMillis();
        lh.bizType = 5;
        lh.setLens(reqData.bizHeaderSize, reqData.bizBodySize);
        CRC32C crc32c = new CRC32C();
        ByteBuffer crcBuf = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE - 4);
        lh.computeCrc(crc32c, crcBuf);

        return new RaftTask(lh, reqData, null, null, false);
    }

    private static int calcCrc(RefBuffer buf) {
        if (buf == null) {
            return 0;
        }
        CRC32C c = new CRC32C();
        ByteBuffer bb = buf.getBuffer();
        int pos = bb.position();
        c.update(bb);
        bb.position(pos);
        return (int) c.getValue();
    }

    public static void assertData(RaftLogData data, int expectedHeaderLen, int expectedBodyLen) {
        assertNotNull(data);
        LogHeader lh = data.logHeader;
        assertEquals(LogHeader.TYPE_NORMAL, lh.type);
        assertEquals(5, lh.bizType);
        assertEquals(1, lh.term);
        assertEquals(0, lh.prevLogTerm);
        assertEquals(100, lh.index);
        assertEquals(LogHeader.computeTotalLen(expectedHeaderLen, expectedBodyLen), lh.totalLen);

        if (expectedHeaderLen > 0) {
            assertNotNull(data.bizHeader);
            ByteBuffer hb = data.bizHeader.getBuffer();
            assertEquals(expectedHeaderLen, hb.remaining());
            for (int i = 0; i < expectedHeaderLen; i++) {
                assertEquals((byte) i, hb.get(i));
            }
            assertTrue(data.bizHeaderCrc != 0);
        } else {
            assertNull(data.bizHeader);
            assertEquals(0, data.bizHeaderCrc);
        }

        if (expectedBodyLen > 0) {
            assertNotNull(data.bizBody);
            ByteBuffer bb = data.bizBody.getBuffer();
            assertEquals(expectedBodyLen, bb.remaining());
            for (int i = 0; i < expectedBodyLen; i++) {
                assertEquals((byte) i, bb.get(i));
            }
            assertTrue(data.bizBodyCrc != 0);
        } else {
            assertNull(data.bizBody);
            assertEquals(0, data.bizBodyCrc);
        }
    }
}
