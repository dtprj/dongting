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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.common.Timestamp;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author huangli
 */
public class StringFieldDecoderTest {

    private StringFieldDecoder decoder;
    private byte[] bytes;
    private ByteBuffer buf;

    @Test
    public void test() {
        decoder = new StringFieldDecoder(new SimpleByteBufferPool(new Timestamp(), false));
        bytes = new byte[33 * 1024];
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
        String s = decoder.decodeUTF8(buf, 100, true, true);
        assertEquals(new String(bytes, 0, 100), s);

        buf.clear();
        s = decoder.decodeUTF8(buf, buf.capacity(), true, true);
        assertEquals(new String(bytes, 0, buf.capacity()), s);
    }

    private void testHalf1() {
        buf.clear();
        buf.limit(10);
        assertNull(decoder.decodeUTF8(buf, 100, true, false));
        buf.clear();
        buf.position(10);
        buf.limit(100);
        String s = decoder.decodeUTF8(buf, 100, false, true);
        assertEquals(new String(bytes, 0, 100), s);

        buf.clear();
        buf.limit(10);
        assertNull(decoder.decodeUTF8(buf, buf.capacity(), true, false));
        buf.clear();
        buf.position(10);
        buf.limit(buf.capacity());
        s = decoder.decodeUTF8(buf, buf.capacity(), false, true);
        assertEquals(new String(bytes, 0, buf.capacity()), s);
    }

    private void testHalf2() {
        buf.clear();
        buf.limit(10);
        assertNull(decoder.decodeUTF8(buf, 100, true, false));
        buf.clear();
        buf.position(10);
        buf.limit(20);
        assertNull(decoder.decodeUTF8(buf, 100, false, false));
        buf.clear();
        buf.position(20);
        buf.limit(100);
        String s = decoder.decodeUTF8(buf, 100, false, true);
        assertEquals(new String(bytes, 0, 100), s);

        buf.clear();
        buf.limit(10);
        assertNull(decoder.decodeUTF8(buf, buf.capacity(), true, false));
        buf.clear();
        buf.position(10);
        buf.limit(20);
        assertNull(decoder.decodeUTF8(buf, buf.capacity(), false, false));
        buf.clear();
        buf.position(20);
        buf.limit(buf.capacity());
        s = decoder.decodeUTF8(buf, buf.capacity(), false, true);
        assertEquals(new String(bytes, 0, buf.capacity()), s);
    }
}
