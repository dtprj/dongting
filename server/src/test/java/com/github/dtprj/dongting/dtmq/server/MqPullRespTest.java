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

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.dtmq.DtMsg;
import com.github.dtprj.dongting.dtmq.MqPullResp;
import com.github.dtprj.dongting.dtmq.MqPullRespDetail;
import com.github.dtprj.dongting.dtmq.codec.MqPullRespCallback;
import com.github.dtprj.dongting.dtmq.proto.DtMq;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.util.CodecTestUtil;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MqPullRespTest {
    private static final byte[] HEADER = {7, 0, -1, 3};

    @Test
    void testDecode() {
        long ts1 = 0x123456789ABL;
        long ts2 = 0x23456789ABCL;
        long ts3 = 0x3456789ABCDL;
        byte[] r1 = record(11, ts1, HEADER, new byte[]{1, 2, 3});
        byte[] r2 = record(11, ts2, new byte[0], new byte[]{4, 5});
        byte[] r3 = record(22, ts3, new byte[]{6}, new byte[0]);
        byte[] data = response(new DtMq.MqPullRespDetail[]{
                detail(11, 100, 2, false),
                detail(22, 7, 1, false),
                detail(33, 5, 0, true)}, r1, r2, r3);

        assertDecoded(decode(data, data.length));
        assertDecoded(decode(data, 1));
        assertDecoded(decode(data, 3));
        assertDecoded(decode(data, 7));
        // every possible two-chunk split, including totalLen, the 12-byte head and both crc fields
        for (int i = 1; i < data.length; i++) {
            assertDecoded(decodeSplit(data, i));
        }
    }

    @Test
    void testDecodeEmptyHeaderAndBody() {
        byte[] r1 = record(11, 1, null, null);
        byte[] r2 = record(11, 2, HEADER, null);
        byte[] r3 = record(11, 3, null, new byte[]{1, 2, 3});
        byte[] r4 = record(11, 4, new byte[]{4}, new byte[]{5});
        byte[] data = response(new DtMq.MqPullRespDetail[]{
                detail(11, 0, 4, false)}, r1, r2, r3, r4);

        MqPullResp resp = decode(data, 2);
        assertEquals(4, resp.records.size());
        assertArrayEquals(new byte[0], resp.records.get(0).header);
        assertArrayEquals(new byte[0], resp.records.get(0).body);
        assertArrayEquals(HEADER, resp.records.get(1).header);
        assertArrayEquals(new byte[0], resp.records.get(1).body);
        assertArrayEquals(new byte[0], resp.records.get(2).header);
        assertArrayEquals(new byte[]{1, 2, 3}, resp.records.get(2).body);
        assertArrayEquals(new byte[]{4}, resp.records.get(3).header);
        assertArrayEquals(new byte[]{5}, resp.records.get(3).body);
        for (int i = 0; i < resp.records.size(); i++) {
            assertEquals(i, resp.records.get(i).seq);
        }
    }

    @Test
    void testDecodeEmpty() {
        MqPullResp resp = decode(new byte[0], 1);
        assertTrue(resp.results.isEmpty());
        assertTrue(resp.records.isEmpty());

        byte[] data = DtMq.MqPullResp.newBuilder()
                .addResults(detail(11, 0, 0, false))
                .build().toByteArray();
        resp = decode(data, 1);
        assertEquals(1, resp.results.size());
        assertTrue(resp.records.isEmpty());
    }

    @Test
    void testDecodeTruncatedRecords() {
        byte[] record = record(11, 123, new byte[]{1, 2, 3}, new byte[]{4, 5});
        for (int len = 0; len < record.length; len++) {
            byte[] data = response(new DtMq.MqPullRespDetail[]{
                    detail(11, 0, 1, false)}, Arrays.copyOf(record, len));
            assertThrows(PbException.class, () -> decode(data, 7), "truncated at " + len);
        }
        // results present, records field missing
        byte[] data = DtMq.MqPullResp.newBuilder()
                .addResults(detail(11, 0, 1, false))
                .build().toByteArray();
        assertThrows(PbException.class, () -> decode(data, 7));
    }

    @Test
    void testRecordLenExceedsRecordsField() {
        byte[] record = record(11, 1, HEADER, new byte[]{1, 2, 3});
        ByteBuffer.wrap(record).putInt(0, Integer.MAX_VALUE);
        byte[] data = response(new DtMq.MqPullRespDetail[]{
                detail(11, 0, 1, false)}, record);
        assertThrows(PbException.class, () -> decode(data, 7));
    }

    private static void assertDecoded(MqPullResp resp) {
        assertEquals(3, resp.results.size());
        MqPullRespDetail d0 = resp.results.get(0);
        assertEquals(11, d0.queueId);
        assertEquals(100, d0.fromSeq);
        assertEquals(2, d0.count);
        assertFalse(d0.truncated);
        MqPullRespDetail d1 = resp.results.get(1);
        assertEquals(22, d1.queueId);
        assertEquals(7, d1.fromSeq);
        assertEquals(1, d1.count);
        assertFalse(d1.truncated);
        MqPullRespDetail d2 = resp.results.get(2);
        assertEquals(33, d2.queueId);
        assertEquals(5, d2.fromSeq);
        assertEquals(0, d2.count);
        assertTrue(d2.truncated);

        assertEquals(3, resp.records.size());
        DtMsg m0 = resp.records.get(0);
        assertEquals(11, m0.queueId);
        assertEquals(100, m0.seq);
        assertEquals(0x123456789ABL, m0.timestamp);
        assertArrayEquals(HEADER, m0.header);
        assertArrayEquals(new byte[]{1, 2, 3}, m0.body);
        DtMsg m1 = resp.records.get(1);
        assertEquals(11, m1.queueId);
        assertEquals(101, m1.seq);
        assertEquals(0x23456789ABCL, m1.timestamp);
        assertArrayEquals(new byte[0], m1.header);
        assertArrayEquals(new byte[]{4, 5}, m1.body);
        DtMsg m2 = resp.records.get(2);
        assertEquals(22, m2.queueId);
        assertEquals(7, m2.seq);
        assertEquals(0x3456789ABCDL, m2.timestamp);
        assertArrayEquals(new byte[]{6}, m2.header);
        assertArrayEquals(new byte[0], m2.body);
    }

    private static MqPullResp decode(byte[] data, int chunkSize) {
        DecodeContext context = CodecTestUtil.decodeContext();
        DecoderCallback<MqPullResp> callback = context.toDecoderCallback(new MqPullRespCallback());
        Decoder decoder = new Decoder();
        decoder.prepareNext(context, callback);
        MqPullResp result = null;
        int pos = 0;
        do {
            int len = Math.min(chunkSize, data.length - pos);
            ByteBuffer buf = ByteBuffer.wrap(data, pos, len).slice();
            result = (MqPullResp) decoder.decode(buf, data.length, pos);
            pos += len;
        } while (pos < data.length);
        assertTrue(decoder.isFinished());
        return result;
    }

    private static MqPullResp decodeSplit(byte[] data, int split) {
        DecodeContext context = CodecTestUtil.decodeContext();
        DecoderCallback<MqPullResp> callback = context.toDecoderCallback(new MqPullRespCallback());
        Decoder decoder = new Decoder();
        decoder.prepareNext(context, callback);
        ByteBuffer first = ByteBuffer.wrap(data, 0, split).slice();
        decoder.decode(first, data.length, 0);
        ByteBuffer second = ByteBuffer.wrap(data, split, data.length - split).slice();
        MqPullResp result = (MqPullResp) decoder.decode(second, data.length, split);
        assertTrue(decoder.isFinished());
        return result;
    }

    private static byte[] response(DtMq.MqPullRespDetail[] results, byte[]... records) {
        DtMq.MqPullResp.Builder builder = DtMq.MqPullResp.newBuilder()
                .setResultsSize(results.length)
                .addAllResults(Arrays.asList(results));
        if (records.length > 0) {
            builder.setRecords(ByteString.copyFrom(concat(records)));
        }
        return builder.build().toByteArray();
    }

    private static DtMq.MqPullRespDetail detail(long queueId, long fromSeq, int count, boolean truncated) {
        return DtMq.MqPullRespDetail.newBuilder()
                .setQueueId(queueId)
                .setFromSeq(fromSeq)
                .setCount(count)
                .setTruncated(truncated)
                .build();
    }

    private static byte[] record(long queueId, long timestamp, byte[] header, byte[] body) {
        int headerLen = header == null ? 0 : header.length;
        int bodyLen = body == null ? 0 : body.length;
        int totalLen = LogHeader.computeTotalLen(headerLen, bodyLen);
        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        buf.position(LogHeader.ITEM_HEADER_SIZE);
        if (headerLen > 0) {
            buf.put(header);
            CRC32C crc = new CRC32C();
            crc.update(header, 0, headerLen);
            buf.putInt((int) crc.getValue());
        }
        if (bodyLen > 0) {
            buf.put(body);
            CRC32C crc = new CRC32C();
            crc.update(body, 0, bodyLen);
            buf.putInt((int) crc.getValue());
        }
        LogHeader logHeader = new LogHeader();
        logHeader.totalLen = totalLen;
        logHeader.bizHeaderLen = headerLen;
        logHeader.bodyLen = bodyLen;
        logHeader.type = LogHeader.TYPE_NORMAL;
        logHeader.bizType = 1;
        logHeader.timestamp = timestamp;
        logHeader.term = 1;
        logHeader.index = 1;
        logHeader.bizKey = queueId;
        logHeader.writeTo(buf, 0);
        CRC32C crc = new CRC32C();
        RaftUtil.updateCrc(crc, buf, 0, LogHeader.ITEM_HEADER_SIZE - 4);
        buf.putInt(LogHeader.ITEM_HEADER_SIZE - 4, (int) crc.getValue());
        return buf.array();
    }

    private static byte[] concat(byte[]... arrays) {
        int len = 0;
        for (byte[] array : arrays) {
            len += array.length;
        }
        byte[] result = new byte[len];
        int pos = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, result, pos, array.length);
            pos += array.length;
        }
        return result;
    }
}
