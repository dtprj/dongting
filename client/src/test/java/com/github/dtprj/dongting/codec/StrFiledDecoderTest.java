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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.common.Timestamp;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author huangli
 */
public class StrFiledDecoderTest {

    private StrFiledDecoder decoder;
    private byte[] bytes;
    private ByteBuffer buf;
    private DecodeContext decodeContext;

    @Test
    public void test() {
        decodeContext = new DecodeContext();
        ByteBufferPool byteBufferPool = new SimpleByteBufferPool(new Timestamp(), false);
        decodeContext.setHeapPool(new RefBufferFactory(byteBufferPool, 128));
        decoder = StrFiledDecoder.INSTANCE;
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
        String s = decoder.decode(decodeContext, buf, 100, 0);
        assertEquals(new String(bytes, 0, 100), s);
        decoder.finish(decodeContext);

        buf.clear();
        s = decoder.decode(decodeContext, buf, buf.capacity(), 0);
        assertEquals(new String(bytes, 0, buf.capacity()), s);
        decoder.finish(decodeContext);
    }

    private void testHalf1() {
        buf.clear();
        buf.limit(10);
        assertNull(decoder.decode(decodeContext, buf, 100, 0));
        buf.clear();
        buf.position(10);
        buf.limit(100);
        String s = decoder.decode(decodeContext, buf, 100, 10);
        assertEquals(new String(bytes, 0, 100), s);

        buf.clear();
        buf.limit(10);
        assertNull(decoder.decode(decodeContext, buf, buf.capacity(), 0));
        buf.clear();
        buf.position(10);
        buf.limit(buf.capacity());
        s = decoder.decode(decodeContext, buf, buf.capacity(), 10);
        assertEquals(new String(bytes, 0, buf.capacity()), s);
        decoder.finish(decodeContext);
    }

    private void testHalf2() {
        buf.clear();
        buf.limit(10);
        assertNull(decoder.decode(decodeContext, buf, 100, 0));
        buf.clear();
        buf.position(10);
        buf.limit(20);
        assertNull(decoder.decode(decodeContext, buf, 100, 10));
        buf.clear();
        buf.position(20);
        buf.limit(100);
        String s = decoder.decode(decodeContext, buf, 100, 20);
        assertEquals(new String(bytes, 0, 100), s);
        decoder.finish(decodeContext);

        buf.clear();
        buf.limit(10);
        assertNull(decoder.decode(decodeContext, buf, buf.capacity(), 0));
        buf.clear();
        buf.position(10);
        buf.limit(20);
        assertNull(decoder.decode(decodeContext, buf, buf.capacity(), 10));
        buf.clear();
        buf.position(20);
        buf.limit(buf.capacity());
        s = decoder.decode(decodeContext, buf, buf.capacity(), 20);
        assertEquals(new String(bytes, 0, buf.capacity()), s);
        decoder.finish(decodeContext);
    }
}
