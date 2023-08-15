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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class StrFieldEncoderTest {
    @Test
    @SuppressWarnings("ConstantValue")
    public void testNull() {
        StrFieldEncoder e = new StrFieldEncoder();
        String str = null;
        assertEquals(0, e.actualSize(str));
        assertTrue(e.encode(null, null, str));
        assertTrue(new StrFieldEncoder().encode(null, null, str));
    }

    @Test
    public void testEmpty() {
        StrFieldEncoder e = new StrFieldEncoder();
        String str = "";
        assertEquals(0, e.actualSize(str));
        assertTrue(e.encode(null, null, str));
        assertTrue(new StrFieldEncoder().encode(null, null, str));
    }

    @Test
    public void testSimple() {
        StrFieldEncoder e = new StrFieldEncoder();
        String str = "中文abc";
        ByteBuffer buffer = ByteBuffer.allocate(16);

        assertEquals(9, e.actualSize(str));
        assertTrue(e.encode(null, buffer, str));
        assertEquals(9, buffer.position());
        assertEquals(str, new String(buffer.array(), 0, 9));

        buffer.clear();
        assertTrue(new StrFieldEncoder().encode(null, buffer, str));
        assertEquals(9, buffer.position());
        assertEquals(str, new String(buffer.array(), 0, 9));
    }

    @Test
    public void testSmallBuffer() {
        StrFieldEncoder e = new StrFieldEncoder();
        String str = "中文abc";
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.limit(5);

        assertEquals(9, e.actualSize(str));
        assertFalse(e.encode(null, buffer, str));
        assertEquals(5, buffer.position());
        buffer.limit(16);
        buffer.position(5);
        assertTrue(e.encode(null, buffer, str));
        assertEquals(9, buffer.position());
        assertEquals(str, new String(buffer.array(), 0, 9));

        buffer.clear();
        buffer.limit(5);
        e = new StrFieldEncoder();
        assertFalse(e.encode(null, buffer, str));
        assertEquals(5, buffer.position());
        buffer.limit(16);
        buffer.position(5);
        assertTrue(e.encode(null, buffer, str));
        assertEquals(9, buffer.position());
        assertEquals(str, new String(buffer.array(), 0, 9));
    }
}
