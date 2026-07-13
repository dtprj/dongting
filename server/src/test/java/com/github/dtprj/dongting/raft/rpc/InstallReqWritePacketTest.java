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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.buf.SimpleByteBufferPoolConfig;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbParser;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class InstallReqWritePacketTest {

    private byte[] expectedData;

    @Test
    public void testEncode_Heap() {
        testEncode0(20, false);
    }

    @Test
    public void testEncode_DirectBuffer() {
        testEncode0(20, true);
    }

    @Test
    public void testEncode_NoData() {
        testEncode0(0, false);
    }

    private void testEncode0(int dataSize, boolean useDirect) {
        InstallSnapshotReq req = createReq(dataSize, useDirect);
        InstallSnapshotReq.InstallReqWritePacket f = new InstallSnapshotReq.InstallReqWritePacket(req);
        f.groupId = req.groupId;

        ByteBuffer body;
        if (dataSize > 0) {
            assertTrue(f.getTotalPreEncodedBufferSize() > 0);
            body = encodeBodyCollectPreEncoded(f);
        } else {
            assertEquals(0, f.getTotalPreEncodedBufferSize());
            body = ByteBuffer.allocate(f.actualBodySize());
            assertTrue(f.encodeBody(new EncodeContext(null), body));
            body.flip();
        }

        InstallSnapshotReq result = decode(f.actualBodySize(), body);
        check(req, result);
    }

    @Test
    public void testSmallBufferEncode_Heap() {
        for (int i = 0; i < 100; i++) {
            testSmallBufferEncode0(20, false);
        }
    }

    @Test
    public void testSmallBufferEncode_DirectBuffer() {
        for (int i = 0; i < 100; i++) {
            testSmallBufferEncode0(20, true);
        }
    }

    @Test
    public void testSmallBufferEncode_NoData() {
        for (int i = 0; i < 100; i++) {
            testSmallBufferEncode0(0, false);
        }
    }

    private void testSmallBufferEncode0(int dataSize, boolean useDirect) {
        InstallSnapshotReq req = createReq(dataSize, useDirect);
        InstallSnapshotReq.InstallReqWritePacket f = new InstallSnapshotReq.InstallReqWritePacket(req);
        f.groupId = req.groupId;

        EncodeContext context = new EncodeContext(null);
        int actualBodySize = f.actualBodySize();
        PbParser p = new PbParser();
        p.prepareNext(CodecTestUtil.decodeContext(), new InstallSnapshotReq.Callback(), actualBodySize);
        Random r = new Random();

        int totalInline = 0;
        InstallSnapshotReq parseResult = null;
        while (true) {
            int size = r.nextBoolean() ? r.nextInt(5) + 1 : r.nextInt(100) + 1;
            ByteBuffer buf = ByteBuffer.allocate(size);
            boolean finished = f.encodeBody(context, buf);
            buf.flip();
            if (buf.remaining() > 0) {
                totalInline += buf.remaining();
                parseResult = (InstallSnapshotReq) p.parse(buf);
            }
            if (finished) {
                break;
            }
            if (f.hasPreEncodedBuffer()) {
                ByteBuffer preBuf = f.getPreEncodedBuffer();
                assertNotNull(preBuf);
                assertTrue(preBuf.remaining() > 0);
                parseResult = (InstallSnapshotReq) p.parse(preBuf);
            }
        }
        assertEquals(actualBodySize, totalInline + f.getTotalPreEncodedBufferSize());
        check(req, parseResult);
    }

    private ByteBuffer encodeBodyCollectPreEncoded(InstallSnapshotReq.InstallReqWritePacket f) {
        EncodeContext context = new EncodeContext(null);
        int actualBodySize = f.actualBodySize();
        ByteBuffer dest = ByteBuffer.allocate(actualBodySize);
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

    private InstallSnapshotReq decode(int actualBodySize, ByteBuffer body) {
        InstallSnapshotReq.Callback c = new InstallSnapshotReq.Callback();
        PbParser p = new PbParser();
        p.prepareNext(CodecTestUtil.decodeContext(), c, actualBodySize);
        return (InstallSnapshotReq) p.parse(body);
    }

    private InstallSnapshotReq createReq(int dataSize, boolean useDirect) {
        InstallSnapshotReq req = new InstallSnapshotReq();
        req.groupId = 12345;
        req.term = 4;
        req.leaderId = 2;
        req.lastIncludedIndex = 1000;
        req.lastIncludedTerm = 5;
        req.offset = 0;
        req.done = false;
        req.nextWritePos = 2000;
        req.members = new HashSet<>();
        req.members.add(1);
        req.members.add(2);
        req.observers = new HashSet<>();
        req.observers.add(3);
        req.preparedMembers = new HashSet<>();
        req.preparedObservers = new HashSet<>();
        req.lastConfigChangeIndex = 500;

        if (dataSize > 0) {
            expectedData = new byte[dataSize];
            new Random().nextBytes(expectedData);
            RefBuffer rb = createRefBuffer(expectedData, useDirect);
            rb.prepareForEncode();
            req.data = rb;
        } else {
            expectedData = null;
            req.data = null;
        }
        return req;
    }

    private RefBuffer createRefBuffer(byte[] data, boolean useDirect) {
        if (useDirect) {
            SimpleByteBufferPoolConfig poolConfig = new SimpleByteBufferPoolConfig(
                    true, data.length, new int[]{data.length + 1}, new int[]{0}, new int[]{1});
            SimpleByteBufferPool pool = new SimpleByteBufferPool(poolConfig);
            RefBuffer rb = pool.borrow(false, data.length);
            rb.getBuffer().put(data);
            rb.getBuffer().flip();
            return rb;
        } else {
            ByteBuffer buf = ByteBuffer.allocate(data.length);
            buf.put(data);
            buf.flip();
            return RefBuffer.wrap(buf);
        }
    }

    private void check(InstallSnapshotReq expect, InstallSnapshotReq result) {
        assertEquals(expect.groupId, result.groupId);
        assertEquals(expect.term, result.term);
        assertEquals(expect.leaderId, result.leaderId);
        assertEquals(expect.lastIncludedIndex, result.lastIncludedIndex);
        assertEquals(expect.lastIncludedTerm, result.lastIncludedTerm);
        assertEquals(expect.offset, result.offset);
        assertEquals(expect.done, result.done);
        assertEquals(expect.nextWritePos, result.nextWritePos);
        assertEquals(expect.lastConfigChangeIndex, result.lastConfigChangeIndex);
        assertEquals(expect.members, result.members);
        assertEquals(expect.observers, result.observers);

        if (expectedData != null) {
            assertNotNull(result.data);
            ByteBuffer rb = result.data.getBuffer();
            assertEquals(expectedData.length, rb.remaining());
            int pos = rb.position();
            for (int i = 0; i < expectedData.length; i++) {
                assertEquals(expectedData[i], rb.get(pos + i));
            }
        }
    }
}
