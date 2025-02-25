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

import com.github.dtprj.dongting.codec.CodecTestUtil;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.codec.PbUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class MultiParserTest {

    private static class Callback extends PbCallback<Object> {
        int beginCount;
        int endSuccessCount;
        int endFailCount;

        String f1;

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
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            f1 = parseUTF8(buf, fieldLen, currentPos);
            return true;
        }

        @Override
        protected Object getResult() {
            return this;
        }
    }

    @Test
    public void testParse() {
        Callback callback = new Callback();
        ByteBuffer buf = ByteBuffer.allocate(64);
        buf.putInt(0);
        PbUtil.writeAsciiField(buf, 1, "VVV");
        // len has 4 bytes
        int len = buf.position() - 4;
        buf.putInt(0, len);
        buf.flip();

        DecodeContext c = CodecTestUtil.createContext();
        MultiParser parser = new MultiParser(c, callback, len);
        parser.parse(buf);
        assertEquals("VVV", callback.f1);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        buf.position(0);
        parser.parse(buf);
        assertEquals(2, callback.beginCount);
        assertEquals(2, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);
    }

    @Test
    public void testPbLenOverflow1() {
        Callback callback = new Callback();
        ByteBuffer buf = ByteBuffer.allocate(64);
        buf.putInt(0);
        PbUtil.writeAsciiField(buf, 1, "VVV");
        // len has 4 bytes
        int len = buf.position() - 4;
        buf.putInt(0, len);
        buf.flip();

        DecodeContext c = CodecTestUtil.createContext();
        MultiParser parser = new MultiParser(c, callback, len);
        parser.parse(buf);
        assertEquals("VVV", callback.f1);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        buf.position(0);
        parser = new MultiParser(c, callback, len - 1);
        try {
            parser.parse(buf);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("maxSize exceed"));
        }
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);
    }

    @Test
    public void testPbLenOverflow2() {
        Callback callback = new Callback();
        ByteBuffer buf = ByteBuffer.allocate(64);
        buf.putInt(0);
        PbUtil.writeAsciiField(buf, 1, "VVV");
        // len has 4 bytes
        int len = buf.position() - 4;
        buf.putInt(0, len);
        buf.flip();

        DecodeContext c = CodecTestUtil.createContext();
        MultiParser parser = new MultiParser(c, callback, len);
        parseByByte(buf, parser);
        assertEquals("VVV", callback.f1);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        buf.position(0);
        parser = new MultiParser(c, callback, len - 1);
        try {
            parseByByte(buf, parser);
            fail();
        } catch (PbException e) {
            assertTrue(e.getMessage().startsWith("maxSize exceed"));
        }
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);
    }

    private static void parseByByte(ByteBuffer buf, MultiParser parser) {
        while (buf.remaining() > 0) {
            byte[] bs = new byte[1];
            bs[0] = buf.get();
            parser.parse(ByteBuffer.wrap(bs));
        }
    }
}
