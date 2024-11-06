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
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class StrEncoderTest {

    @Test
    public void testEmpty() {
        StrEncoder e = new StrEncoder("");
        assertEquals(0, e.actualSize());
        assertTrue(e.encode(new EncodeContext(null), ByteBuffer.allocate(0)));
    }

    @Test
    public void testSimple() {
        StrEncoder e = new StrEncoder("中文abc");
        ByteBuffer buffer = ByteBuffer.allocate(16);

        EncodeContext context = new EncodeContext(null);

        assertEquals(9, e.actualSize());
        assertTrue(e.encode(context, buffer));
        assertEquals(9, buffer.position());
        assertEquals(e.getStr(), new String(buffer.array(), 0, 9, StandardCharsets.UTF_8));

        buffer.clear();
        context.reset();

        assertEquals(9, e.actualSize());
        assertTrue(e.encode(context, buffer));
        assertEquals(9, buffer.position());
        assertEquals(e.getStr(), new String(buffer.array(), 0, 9, StandardCharsets.UTF_8));
    }

    @Test
    public void testSmallBuffer() {
        EncodeContext context = new EncodeContext(null);

        StrEncoder e = new StrEncoder("中文abc");
        ByteBuffer buffer = ByteBuffer.allocate(16);

        buffer.limit(5);
        assertEquals(9, e.actualSize());
        assertFalse(e.encode(context, buffer));
        assertEquals(5, buffer.position());
        buffer.limit(16);
        buffer.position(5);
        assertTrue(e.encode(context, buffer));
        assertEquals(9, buffer.position());
        assertEquals(e.getStr(), new String(buffer.array(), 0, 9, StandardCharsets.UTF_8));

        buffer.clear();
        context.reset();

        buffer.limit(5);
        assertEquals(9, e.actualSize());
        assertFalse(e.encode(context, buffer));
        assertEquals(5, buffer.position());
        buffer.limit(16);
        buffer.position(5);
        assertTrue(e.encode(context, buffer));
        assertEquals(9, buffer.position());
        assertEquals(e.getStr(), new String(buffer.array(), 0, 9, StandardCharsets.UTF_8));
    }
}
