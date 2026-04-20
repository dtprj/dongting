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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftTaskTest;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftLogDataCallbackTest {

    @Test
    void testFullBufferHeaderOnly() {
        decodeAndVerifyFullBuffer(0, 0);
    }

    @Test
    void testFullBufferWithBizHeader() {
        decodeAndVerifyFullBuffer(10, 0);
    }

    @Test
    void testFullBufferWithBizBody() {
        decodeAndVerifyFullBuffer(0, 20);
    }

    @Test
    void testFullBufferWithBizHeaderAndBody() {
        decodeAndVerifyFullBuffer(10, 20);
    }

    @Test
    void testSmallBufferHeaderOnly() {
        decodeAndVerifySmallBuffer(0, 0);
    }

    @Test
    void testSmallBufferWithBizHeader() {
        decodeAndVerifySmallBuffer(10, 0);
    }

    @Test
    void testSmallBufferWithBizBody() {
        decodeAndVerifySmallBuffer(0, 20);
    }

    @Test
    void testSmallBufferWithBizHeaderAndBody() {
        decodeAndVerifySmallBuffer(10, 20);
    }

    @Test
    void testSmallBufferWithLargePayload() {
        decodeAndVerifySmallBuffer(100, 200);
    }

    @Test
    void testMultipleEntriesInOneBuffer() {
        RaftTask task1 = RaftTaskTest.createTask(5, 10);
        RaftTask task2 = RaftTaskTest.createTask(8, 15);
        ByteBuffer buf1 = CodecTestUtil.fullBufferEncode(task1);
        ByteBuffer buf2 = CodecTestUtil.fullBufferEncode(task2);

        ByteBuffer combined = ByteBuffer.allocate(buf1.remaining() + buf2.remaining());
        combined.put(buf1);
        combined.put(buf2);
        combined.flip();

        List<RaftLogData> results = new ArrayList<>();
        RaftLogDataCallback callback = new RaftLogDataCallback(results::add);
        Decoder decoder = new Decoder();
        decoder.prepareNext(CodecTestUtil.decodeContext(), callback);
        assertTrue(callback.doDecode(combined, 0, 0));
        assertEquals(2, results.size());

        RaftTaskTest.assertData(results.get(0), 5, 10);
        RaftTaskTest.assertData(results.get(1), 8, 15);

        results.get(0).release();
        results.get(1).release();
    }

    private void decodeAndVerifyFullBuffer(int bizHeaderLen, int bodyLen) {
        RaftTask task = RaftTaskTest.createTask(bizHeaderLen, bodyLen);
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(task);

        List<RaftLogData> results = new ArrayList<>();
        RaftLogDataCallback callback = new RaftLogDataCallback(results::add);
        Decoder decoder = new Decoder();
        decoder.prepareNext(CodecTestUtil.decodeContext(), callback);
        decoder.decode(buf, buf.remaining(), 0);
        assertTrue(decoder.isFinished());
        assertEquals(1, results.size());
        RaftTaskTest.assertData(results.get(0), bizHeaderLen, bodyLen);
        results.get(0).release();
    }

    private void decodeAndVerifySmallBuffer(int bizHeaderLen, int bodyLen) {
        RaftTask task = RaftTaskTest.createTask(bizHeaderLen, bodyLen);
        ByteBuffer encoded = CodecTestUtil.fullBufferEncode(task);

        List<RaftLogData> results = new ArrayList<>();
        RaftLogDataCallback callback = new RaftLogDataCallback(results::add);
        Decoder decoder = new Decoder();
        decoder.prepareNext(CodecTestUtil.decodeContext(), callback);

        int totalSize = encoded.remaining();
        int splitSize = 3;
        int decodedBytes = 0;
        while (decodedBytes < totalSize) {
            int chunkLen = Math.min(splitSize, totalSize - decodedBytes);
            ByteBuffer dup = encoded.duplicate();
            dup.position(decodedBytes);
            dup.limit(decodedBytes + chunkLen);
            ByteBuffer chunk = dup.slice();

            decoder.decode(chunk, totalSize, decodedBytes);
            decodedBytes += chunkLen;
        }

        assertTrue(decoder.isFinished());
        assertEquals(1, results.size());
        RaftTaskTest.assertData(results.get(0), bizHeaderLen, bodyLen);
        results.get(0).release();
    }
}
