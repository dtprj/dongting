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
package com.github.dtprj.dongting.codec;

import com.github.dtprj.dongting.buf.Buffers;
import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.common.DtThread;
import com.github.dtprj.dongting.common.Timestamp;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class StrDecoderCallbackTest {

    private Decoder decoder;
    private StrDecoderCallback callback;
    private byte[] bytes;
    private ByteBuffer buf;
    private DecodeContext decodeContext;

    @Test
    public void test() {
        decodeContext = CodecTestUtil.createContext();
        decoder = new Decoder();
        callback = new StrDecoderCallback();
        bytes = new byte[5 * 1024];
        byte c = 'a';
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = c++;
            if (c > 'z') {
                c = 'a';
            }
        }
        buf = ByteBuffer.wrap(bytes);

        testFull();
        testHalf1();
        testHalf2();
        testFull();
        testHalf2();
        testFull();
        testHalf1();
    }

    private void testFull() {
        buf.clear();
        buf.limit(100);
        decoder.prepareNext(decodeContext, callback);
        String s = (String) decoder.decode(buf, 100, 0);
        assertEquals(new String(bytes, 0, 100), s);

        buf.clear();
        decoder.prepareNext(decodeContext, callback);
        s = (String) decoder.decode(buf, buf.capacity(), 0);
        assertEquals(new String(bytes, 0, buf.capacity()), s);
    }

    private void testHalf1() {
        buf.clear();
        buf.limit(10);
        decoder.prepareNext(decodeContext, callback);
        assertNull(decoder.decode(buf, 100, 0));
        buf.clear();
        buf.position(10);
        buf.limit(100);
        String s = (String) decoder.decode(buf, 100, 10);
        assertEquals(new String(bytes, 0, 100), s);

        buf.clear();
        buf.limit(10);
        decoder.prepareNext(decodeContext, callback);
        assertNull(decoder.decode(buf, buf.capacity(), 0));
        buf.clear();
        buf.position(10);
        buf.limit(buf.capacity());
        s = (String) decoder.decode(buf, buf.capacity(), 10);
        assertEquals(new String(bytes, 0, buf.capacity()), s);
    }

    private void testHalf2() {
        buf.clear();
        buf.limit(10);
        decoder.prepareNext(decodeContext, callback);
        assertNull(decoder.decode(buf, 100, 0));
        buf.clear();
        buf.position(10);
        buf.limit(20);
        assertNull(decoder.decode(buf, 100, 10));
        buf.clear();
        buf.position(20);
        buf.limit(100);
        String s = (String) decoder.decode(buf, 100, 20);
        assertEquals(new String(bytes, 0, 100), s);

        buf.clear();
        buf.limit(10);
        decoder.prepareNext(decodeContext, callback);
        assertNull(decoder.decode(buf, buf.capacity(), 0));
        buf.clear();
        buf.position(10);
        buf.limit(20);
        assertNull(decoder.decode(buf, buf.capacity(), 10));
        buf.clear();
        buf.position(20);
        buf.limit(buf.capacity());
        s = (String) decoder.decode(buf, buf.capacity(), 20);
        assertEquals(new String(bytes, 0, buf.capacity()), s);
    }

    // ---- regression: RefBuffer must be released when parsing is cancelled mid-field ----

    @Test
    public void decoderAbortReleasesBuffer() {
        List<RefBuffer> borrowed = new ArrayList<>();
        DecodeContext ctx = countingContext(borrowed);
        byte[] d = asciiData(100);
        Decoder dec = new Decoder();
        StrDecoderCallback cb = new StrDecoderCallback();
        dec.prepareNext(ctx, cb);
        // only first 30 bytes of a 100-byte field, parsing is not finished
        assertNull(dec.decode(ByteBuffer.wrap(d, 0, 30), 100, 0));
        // simulate cancellation: reset -> StrDecoderCallback.end(false) releases the buffer
        ctx.reset(dec);
        assertAllReleased(borrowed);
    }

    @Test
    public void decoderNormalCompletionReleasesBuffer() {
        List<RefBuffer> borrowed = new ArrayList<>();
        DecodeContext ctx = countingContext(borrowed);
        byte[] d = asciiData(5000); // >= THREAD_LOCAL_BUFFER_SIZE, single buffer, borrows tempRef
        Decoder dec = new Decoder();
        StrDecoderCallback cb = new StrDecoderCallback();
        dec.prepareNext(ctx, cb);
        String s = (String) dec.decode(ByteBuffer.wrap(d), 5000, 0);
        assertEquals(new String(d), s);
        assertAllReleased(borrowed);
    }

    @Test
    public void decoderMultiBufferCompletionReleasesBuffer() {
        // fieldLen=100 (>= 64) spans two buffers; tempRef is released on the final chunk
        List<RefBuffer> borrowed = new ArrayList<>();
        DecodeContext ctx = countingContext(borrowed);
        byte[] d = asciiData(100);
        Decoder dec = new Decoder();
        StrDecoderCallback cb = new StrDecoderCallback();
        dec.prepareNext(ctx, cb);
        // first chunk: 30 bytes, field not finished, tempRef borrowed and held
        assertNull(dec.decode(ByteBuffer.wrap(d, 0, 30), 100, 0));
        // second chunk: remaining 70 bytes, completes the field and releases tempRef
        String s = (String) dec.decode(ByteBuffer.wrap(d, 30, 70), 100, 30);
        assertEquals(new String(d), s);
        assertAllReleased(borrowed);
    }

    @Test
    public void pbParserAbortReleasesBuffer() {
        // covers the parseUTF8 nested delegation path (e.g. from PbStrCallback)
        List<RefBuffer> borrowed = new ArrayList<>();
        DecodeContext ctx = countingContext(borrowed);
        byte[] fieldBytes = asciiData(100);
        int totalSize = PbUtil.sizeOfBytesField(1, fieldBytes);
        ByteBuffer full = ByteBuffer.allocate(totalSize);
        PbUtil.writeBytesField(full, 1, fieldBytes);
        full.flip();

        PbParser parser = new PbParser();
        PbStrCallback cb = new PbStrCallback();
        parser.prepareNext(ctx, cb, totalSize);

        // feed only the first 50 bytes (tag + len + 48 body bytes); the field is not finished
        ByteBuffer part = full.duplicate();
        part.position(0);
        part.limit(50);
        assertNull(parser.parse(part));

        // simulate cancellation: reset cascades to nested decoder -> StrDecoderCallback.end(false)
        ctx.reset(parser);
        assertAllReleased(borrowed);
    }

    private static DecodeContext countingContext(List<RefBuffer> borrowed) {
        ByteBufferPool heap = new DefaultPoolFactory().createPool(new Timestamp(), false);
        ByteBufferPool direct = new DefaultPoolFactory().createPool(new Timestamp(), true);
        Buffers buffers = new Buffers(heap, direct, heap, direct) {
            @Override
            public RefBuffer borrowRefBuffer(int requestSize, boolean plain, boolean threadSafeRelease, int threshold) {
                RefBuffer rb = super.borrowRefBuffer(requestSize, plain, threadSafeRelease, threshold);
                borrowed.add(rb);
                return rb;
            }
        };
        return new DecodeContext(buffers, new byte[DtThread.THREAD_LOCAL_BUFFER_SIZE]);
    }

    private static byte[] asciiData(int len) {
        byte[] b = new byte[len];
        byte c = 'a';
        for (int i = 0; i < len; i++) {
            b[i] = c++;
            if (c > 'z') {
                c = 'a';
            }
        }
        return b;
    }

    private static void assertAllReleased(List<RefBuffer> borrowed) {
        for (int i = 0; i < borrowed.size(); i++) {
            assertTrue(borrowed.get(i).isReleased(),
                    "RefBuffer #" + i + " is not released (leak)");
        }
    }
}
