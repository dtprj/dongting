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
package com.github.dtprj.dongting.pb;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
public class PbParserTest {

    static class Callback implements PbCallback {

        private int beginCount;
        private int endCount;

        private String msg = "";
        private String body = "";

        private int f1;
        private long f2;
        private String f3;
        private String f4;
        private int expectLen;

        Callback(int f1, long f2, String f3, String f4) {
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
            this.f4 = f4;
        }

        public ByteBuffer buildFrame() {
            byte[] bs = DtPbTest.PbParserTestMsg.newBuilder()
                    .setInt32Field(f1)
                    .setInt64Field(f2)
                    .setStringField(f3)
                    .setBytesField(ByteString.copyFrom(f4.getBytes()))
                    .build()
                    .toByteArray();
            ByteBuffer buf = ByteBuffer.allocate(bs.length + 4);
            buf.putInt(bs.length);
            buf.put(bs);
            buf.flip();
            this.expectLen = bs.length;
            buf.order(ByteOrder.LITTLE_ENDIAN);
            return buf;
        }

        @Override
        public void begin(int len) {
            assertEquals(expectLen, len);
            beginCount++;
        }

        @Override
        public void end() {
            endCount++;
        }

        @Override
        public boolean readVarInt(int index, long value) {
            if (index == 1) {
                assertEquals(f1, (int) value);
            } else if (index == 2) {
                assertEquals(f2, value);
            } else {
                fail();
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int len, boolean begin, boolean end) {
            byte[] bs = new byte[buf.remaining()];
            buf.get(bs);
            String s = new String(bs);
            if (index == 3) {
                msg += s;
                if (end) {
                    assertEquals(f3, msg);
                }
            } else if (index == 4000) {
                body += s;
                if (end) {
                    assertEquals(f4, body);
                }
            } else {
                fail();
            }
            return true;
        }
    }


    @Test
    public void testParse() {
        PbParser parser = new PbParser(500);
        Callback callback = new Callback(100, 200, "msg", "body");
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endCount);

        callback = new Callback(Integer.MAX_VALUE, Long.MAX_VALUE, "msg", "body");
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endCount);

        callback = new Callback(-1, -1, "msg", "body");
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endCount);

        callback = new Callback(Integer.MAX_VALUE, Long.MAX_VALUE, "msg", "body");
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endCount);

        try {
            callback = new Callback(1, 2, "msg", "body");
            new PbParser(5).parse(callback.buildFrame(), callback);
            fail();
        } catch (PbException e) {
        }
        assertEquals(0, callback.beginCount);
        assertEquals(0, callback.endCount);
    }

    @Test
    public void testHalfParse() {
        PbParser parser = new PbParser(500);
        for (int i = 0; i < 2; i++) {
            Callback callback = new Callback(10000, 20000, "msg", "body");
            ByteBuffer fullBuffer = callback.buildFrame();
            for (int j = 0; j < fullBuffer.remaining(); j++) {
                ByteBuffer buf = ByteBuffer.allocate(1);
                buf.put(fullBuffer.get(j));
                buf.flip();
                parser.parse(buf, callback);
            }
            assertEquals(1, callback.beginCount);
            assertEquals(1, callback.endCount);
        }
    }

    @Test
    public void testCallbackFail() {
        PbParser parser = new PbParser(500);

        Callback callback = new Callback(10000, 20000, "msg", "body") {
            @Override
            public boolean readVarInt(int index, long value) {
                if (index == 2) {
                    return false;
                }
                return super.readVarInt(index, value);
            }
        };
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endCount);

        callback = new Callback(10000, 20000, "msg", "body") {
            @Override
            public boolean readVarInt(int index, long value) {
                if (index == 2) {
                    throw new ArrayIndexOutOfBoundsException();
                }
                return super.readVarInt(index, value);
            }
        };
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endCount);

        callback = new Callback(10000, 20000, "msg", "body") {
            @Override
            public boolean readBytes(int index, ByteBuffer buf, int len, boolean begin, boolean end) {
                if (index == 3) {
                    return false;
                }
                return super.readBytes(index, buf, len, begin, end);
            }
        };
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endCount);

        callback = new Callback(10000, 20000, "msg", "body") {
            @Override
            public boolean readBytes(int index, ByteBuffer buf, int len, boolean begin, boolean end) {
                if (index == 3) {
                    throw new ArrayIndexOutOfBoundsException();
                }
                return super.readBytes(index, buf, len, begin, end);
            }
        };
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endCount);

        callback = new Callback(100, 200, "msg", "body");
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endCount);
    }
}
