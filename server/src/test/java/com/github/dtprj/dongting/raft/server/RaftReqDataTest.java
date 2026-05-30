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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.buf.Buffers;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.raft.store.LogHeader;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
class RaftReqDataTest {

    /**
     * Simple Encodable that writes a fixed byte array in one call.
     */
    private static class FixedEncodable implements Encodable {
        private final byte[] data;

        FixedEncodable(byte[] data) {
            this.data = data;
        }

        @Override
        public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
            destBuffer.put(data);
            return true;
        }

        @Override
        public int actualSize() {
            return data.length;
        }
    }

    /**
     * Encodable that encodes in two chunks to exercise the multi-chunk CRC path.
     */
    private static class TwoPhaseEncodable implements Encodable {
        private final byte[] data;
        private boolean firstCall = true;

        TwoPhaseEncodable(byte[] data) {
            this.data = data;
        }

        @Override
        public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
            if (firstCall) {
                // write half and return false
                int half = data.length / 2;
                destBuffer.put(data, 0, half);
                firstCall = false;
                return false;
            } else {
                // write remaining
                int half = data.length / 2;
                destBuffer.put(data, half, data.length - half);
                return true;
            }
        }

        @Override
        public int actualSize() {
            return data.length;
        }
    }

    private static final int TYPE = LogHeader.TYPE_NORMAL;
    private static final int BIZ_TYPE = 1;

    @Test
    void testBuildWithNullBizBody() {
        RaftReqData data = RaftReqData.build(null, TYPE, BIZ_TYPE, null);
        assertNotNull(data);
        assertEquals(LogHeader.ITEM_HEADER_SIZE, data.totalLen);
        assertEquals(0, data.logHeader.bodyLen);
        assertEquals(TYPE, data.logHeader.type);
        assertEquals(BIZ_TYPE, data.logHeader.bizType);
        assertNull(data.prepareReadBizBody());
        data.release();
    }

    @Test
    void testBuildWithZeroSizeBizBody() {
        Encodable emptyBody = new FixedEncodable(new byte[0]);
        RaftReqData data = RaftReqData.build(null, TYPE, BIZ_TYPE, emptyBody);
        assertNotNull(data);
        assertEquals(LogHeader.ITEM_HEADER_SIZE, data.totalLen);
        assertEquals(0, data.logHeader.bodyLen);
        assertNull(data.prepareReadBizBody());
        data.release();
    }

    @Test
    void testBuildWithBodyNoBuffers() {
        byte[] payload = "hello world".getBytes();
        Encodable body = new FixedEncodable(payload);

        RaftReqData data = RaftReqData.build(null, TYPE, BIZ_TYPE, body);
        assertNotNull(data);

        int expectedTotalLen = LogHeader.computeTotalLen(0, payload.length);
        assertEquals(expectedTotalLen, data.totalLen);
        assertEquals(payload.length, data.logHeader.bodyLen);
        assertEquals(TYPE, data.logHeader.type);
        assertEquals(BIZ_TYPE, data.logHeader.bizType);

        // verify body content
        ByteBuffer bizBuf = data.prepareReadBizBody();
        assertNotNull(bizBuf);
        assertEquals(payload.length, bizBuf.remaining());
        byte[] readBack = new byte[payload.length];
        bizBuf.get(readBack);
        assertArrayEquals(payload, readBack);

        // verify CRC is stored after body (4 bytes CRC after body)
        data.reset();
        ByteBuffer fullBuf = data.buffer.getBuffer();
        int bodyEnd = LogHeader.ITEM_HEADER_SIZE + payload.length;
        fullBuf.position(bodyEnd);
        int storedCrc = fullBuf.getInt();

        // compute expected CRC
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        fullBuf.position(LogHeader.ITEM_HEADER_SIZE);
        fullBuf.limit(bodyEnd);
        crc.update(fullBuf);
        assertEquals((int) crc.getValue(), storedCrc);

        data.release();
    }

    @Test
    void testBuildWithBodyWithBuffers() {
        byte[] payload = "test data for buffers".getBytes();
        Encodable body = new FixedEncodable(payload);
        Buffers buffers = new Buffers(null, null, null, null) {
            @Override
            public RefBuffer borrowRefBuffer(int requestSize) {
                ByteBuffer buf = ByteBuffer.allocate(requestSize);
                return RefBuffer.wrap(buf);
            }
        };

        RaftReqData data = RaftReqData.build(buffers, TYPE, BIZ_TYPE, body);
        assertNotNull(data);

        int expectedTotalLen = LogHeader.computeTotalLen(0, payload.length);
        assertEquals(expectedTotalLen, data.totalLen);
        assertEquals(payload.length, data.logHeader.bodyLen);

        ByteBuffer bizBuf = data.prepareReadBizBody();
        assertNotNull(bizBuf);
        byte[] readBack = new byte[payload.length];
        bizBuf.get(readBack);
        assertArrayEquals(payload, readBack);
        data.release();
    }

    @Test
    void testBuildMultiChunkEncode() {
        // Payload larger than ENCODE_CHUNK_SIZE (8192) to exercise multi-chunk CRC loop
        byte[] payload = new byte[9000];
        Arrays.fill(payload, (byte) 0xAB);

        // Encoder that respects buffer remaining() and returns false on first chunk
        Encodable body = new Encodable() {
            int offset = 0;
            @Override
            public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
                int len = Math.min(destBuffer.remaining(), payload.length - offset);
                destBuffer.put(payload, offset, len);
                offset += len;
                return offset == payload.length;
            }
            @Override
            public int actualSize() {
                return payload.length;
            }
        };

        RaftReqData data = RaftReqData.build(null, TYPE, BIZ_TYPE, body);
        assertNotNull(data);

        int expectedTotalLen = LogHeader.computeTotalLen(0, payload.length);
        assertEquals(expectedTotalLen, data.totalLen);
        assertEquals(payload.length, data.logHeader.bodyLen);

        ByteBuffer bizBuf = data.prepareReadBizBody();
        assertNotNull(bizBuf);
        byte[] readBack = new byte[payload.length];
        bizBuf.get(readBack);
        assertArrayEquals(payload, readBack);
        data.release();
    }

    @Test
    void testBuildLargePayloadTwoPhaseEncode() {
        // Two-phase encode with large payload to trigger multi-chunk path
        byte[] payload = new byte[10000];
        Arrays.fill(payload, (byte) 0x55);
        Encodable body = new TwoPhaseEncodable(payload);

        RaftReqData data = RaftReqData.build(null, TYPE, BIZ_TYPE, body);
        assertNotNull(data);

        int expectedTotalLen = LogHeader.computeTotalLen(0, payload.length);
        assertEquals(expectedTotalLen, data.totalLen);

        ByteBuffer bizBuf = data.prepareReadBizBody();
        assertNotNull(bizBuf);
        byte[] readBack = new byte[payload.length];
        bizBuf.get(readBack);
        assertArrayEquals(payload, readBack);
        data.release();
    }

    @Test
    void testBuildWithDifferentTypeAndBizType() {
        byte[] payload = new byte[]{1, 2, 3};
        Encodable body = new FixedEncodable(payload);

        RaftReqData data = RaftReqData.build(null, LogHeader.TYPE_LOG_READ, 99, body);
        assertEquals(LogHeader.TYPE_LOG_READ, data.logHeader.type);
        assertEquals(99, data.logHeader.bizType);
        assertEquals(payload.length, data.logHeader.bodyLen);
        data.release();
    }

    @Test
    void testBuildWithBodyNoBuffersVerifyCrcCorrectness() {
        byte[] payload = "crc check".getBytes();
        Encodable body = new FixedEncodable(payload);

        RaftReqData data = RaftReqData.build(null, TYPE, BIZ_TYPE, body);

        // Read back body + CRC and verify
        data.reset();
        ByteBuffer fullBuf = data.buffer.getBuffer();
        int bodyStart = LogHeader.ITEM_HEADER_SIZE;
        int bodyEnd = bodyStart + payload.length;

        // Verify body content
        fullBuf.position(bodyStart);
        fullBuf.limit(bodyEnd);
        byte[] readBody = new byte[payload.length];
        fullBuf.get(readBody);
        assertArrayEquals(payload, readBody);

        // Verify CRC
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(readBody);
        fullBuf.limit(bodyEnd + 4);
        int storedCrc = fullBuf.getInt();
        assertEquals((int) crc.getValue(), storedCrc);

        data.release();
    }
}
