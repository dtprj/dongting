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

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class PbParserExTest {

    private static void parseByByte(ByteBuffer buf, PbParser parser) {
        while (buf.remaining() > 0) {
            byte[] bs = new byte[1];
            bs[0] = buf.get();
            parser.parse(ByteBuffer.wrap(bs));
        }
    }

    private static class EmptyCallback extends PbCallback<Object> {
        int beginCount;
        int endSuccessCount;
        int endFailCount;

        @Override
        protected void begin(int len) {
            beginCount++;
        }

        @Override
        protected boolean end(boolean success) {
            if (success) {
                endSuccessCount++;
            } else {
                endFailCount++;
            }
            return success;
        }

        @Override
        protected Object getResult() {
            return this;
        }
    }

    @Test
    public void testFieldLenTooLong() {
        ByteBuffer buf = ByteBuffer.allocate(50);
        PbUtil.writeTag(buf, PbUtil.TYPE_LENGTH_DELIMITED, 1);
        buf.put((byte) 0x80);
        buf.put((byte) 0x80);
        buf.put((byte) 0x80);
        buf.put((byte) 0x80);
        buf.put((byte) 0x80);
        buf.put((byte) 1);
        buf.flip();
        buf.mark();

        EmptyCallback callback = new EmptyCallback();
        DecodeContext context = CodecTestUtil.createContext();
        PbParser parser = new PbParser();
        parser.prepareNext(context, callback, buf.remaining());
        try {
            parser.parse(buf);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("var int too long"));
            assertTrue(parser.isFinished());
            assertTrue(parser.shouldSkip());
        }
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);

        buf.reset();
        callback = new EmptyCallback();
        parser.prepareNext(context, callback, buf.remaining());
        try {
            parseByByte(buf, parser);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("var int too long"));
            assertTrue(parser.isFinished());
            assertTrue(parser.shouldSkip());
        }
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);
    }

    @Test
    public void testFieldLenExceed() {
        ByteBuffer buf = ByteBuffer.allocate(50);
        PbUtil.writeTag(buf, PbUtil.TYPE_LENGTH_DELIMITED, 1);
        PbUtil.writeUnsignedInt32ValueOnly(buf, Integer.MAX_VALUE);
        buf.flip();
        buf.mark();

        DecodeContext context = CodecTestUtil.createContext();
        EmptyCallback callback = new EmptyCallback();
        PbParser parser = new PbParser();
        parser.prepareNext(context, callback, buf.remaining() - 1);
        try {
            parser.parse(buf);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("size exceed"));
            assertTrue(parser.isFinished());
            assertTrue(parser.shouldSkip());
        }
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);

        buf.reset();
        callback = new EmptyCallback();
        parser.prepareNext(context, callback, buf.remaining() - 1);
        try {
            parseByByte(buf, parser);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("size exceed"));
            assertTrue(parser.isFinished());
            assertTrue(parser.shouldSkip());
        }
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);
    }

    @Test
    public void testFieldLenOverflow() {
        ByteBuffer buf = ByteBuffer.allocate(50);
        PbUtil.writeTag(buf, PbUtil.TYPE_LENGTH_DELIMITED, 1);
        PbUtil.writeUnsignedInt32ValueOnly(buf, 2);
        PbUtil.writeUnsignedInt32ValueOnly(buf, 1);
        buf.flip();
        buf.mark();

        DecodeContext context = CodecTestUtil.createContext();
        EmptyCallback callback = new EmptyCallback();
        PbParser parser = new PbParser();
        parser.prepareNext(context, callback, buf.remaining());
        try {
            parser.parse(buf);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("field length overflow"));
            assertTrue(parser.isFinished());
            assertTrue(parser.shouldSkip());
        }
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);
    }

    @Test
    public void testBadFieldLen() {
        ByteBuffer buf = ByteBuffer.allocate(50);
        PbUtil.writeTag(buf, PbUtil.TYPE_LENGTH_DELIMITED, 1);
        PbUtil.writeUnsignedInt32ValueOnly(buf, -1);
        PbUtil.writeUnsignedInt32ValueOnly(buf, 1);
        buf.flip();
        buf.mark();

        DecodeContext context = CodecTestUtil.createContext();
        EmptyCallback callback = new EmptyCallback();
        PbParser parser = new PbParser();
        parser.prepareNext(context, callback, buf.remaining());
        try {
            parser.parse(buf);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("bad field len"));
            assertTrue(parser.isFinished());
            assertTrue(parser.shouldSkip());
        }
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);
    }

    @Test
    public void testBadFieldIndex() {
        ByteBuffer buf = ByteBuffer.allocate(50);
        PbUtil.writeUnsignedInt32ValueOnly(buf, 0);
        buf.flip();
        buf.mark();

        DecodeContext context = CodecTestUtil.createContext();
        EmptyCallback callback = new EmptyCallback();
        PbParser parser = new PbParser();
        parser.prepareNext(context, callback, buf.remaining());
        try {
            parser.parse(buf);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("bad index:"));
            assertTrue(parser.isFinished());
            assertTrue(parser.shouldSkip());
        }
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);
    }

    @Test
    public void testFieldValueTooLong() {
        ByteBuffer buf = ByteBuffer.allocate(50);
        PbUtil.writeTag(buf, PbUtil.TYPE_VAR_INT, 1);
        for (int i = 0; i < 10; i++) {
            buf.put((byte) 0x80);
        }
        buf.put((byte) 1);
        buf.flip();
        buf.mark();

        DecodeContext context = CodecTestUtil.createContext();
        EmptyCallback callback = new EmptyCallback();
        PbParser parser = new PbParser();
        parser.prepareNext(context, callback, buf.remaining());
        try {
            parser.parse(buf);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("var long too long"));
            assertTrue(parser.isFinished());
            assertTrue(parser.shouldSkip());
        }
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);

        buf.reset();
        callback = new EmptyCallback();
        parser.prepareNext(context, callback, buf.remaining());
        try {
            parseByByte(buf, parser);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("var long too long"));
            assertTrue(parser.isFinished());
            assertTrue(parser.shouldSkip());
        }
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);
    }

    @Test
    public void testFieldValueExceed() {
        ByteBuffer buf = ByteBuffer.allocate(50);
        PbUtil.writeTag(buf, PbUtil.TYPE_VAR_INT, 1);
        PbUtil.writeUnsignedInt64ValueOnly(buf, Long.MAX_VALUE);
        buf.flip();
        buf.mark();

        DecodeContext context = CodecTestUtil.createContext();
        EmptyCallback callback = new EmptyCallback();
        PbParser parser = new PbParser();
        parser.prepareNext(context, callback, buf.remaining() - 1);
        try {
            parser.parse(buf);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("size exceed"));
            assertTrue(parser.isFinished());
            assertTrue(parser.shouldSkip());
        }
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);

        buf.reset();
        callback = new EmptyCallback();
        parser.prepareNext(context, callback, buf.remaining() - 1);
        try {
            parseByByte(buf, parser);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("size exceed"));
            assertTrue(parser.isFinished());
            assertTrue(parser.shouldSkip());
        }
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);
    }

}
