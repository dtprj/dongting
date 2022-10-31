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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
public class PbParserTest {

    static class Callback implements PbCallback {

        private int beginCount;
        private int endSuccessCount;
        private int endFailCount;

        private int f1;
        private long f2;
        private String f3;
        private String f4;
        private int f5;
        private long f6;
        private int expectLen;

        private int read_f1;
        private long read_f2;
        private String read_f3;
        private String read_f4;
        private int read_f5;
        private long read_f6;

        Callback(int f1, long f2, String f3, String f4, int f5, long f6) {
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
            this.f4 = f4;
            this.f5 = f5;
            this.f6 = f6;
        }

        public ByteBuffer buildFrame() {
            byte[] bs = DtPbTest.PbParserTestMsg.newBuilder()
                    .setInt32Field(f1)
                    .setInt64Field(f2)
                    .setStringField(f3)
                    .setBytesField(ByteString.copyFrom(f4.getBytes()))
                    .setInt32Fix(f5)
                    .setInt64Fix(f6)
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
        public void end(boolean success) {
            if (success) {
                endSuccessCount++;
            } else {
                endFailCount++;
            }
            assertEquals(f1, read_f1);
            assertEquals(f2, read_f2);
            assertEquals(f3, read_f3);
            assertEquals(f4, read_f4);
            assertEquals(f5, read_f5);
            assertEquals(f6, read_f6);
        }

        @Override
        public boolean readVarInt(int index, long value) {
            if (index == 1) {
                read_f1 = (int) value;
            } else if (index == 2) {
                read_f2 = value;
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
                if (read_f3 == null) {
                    read_f3 = "";
                }
                read_f3 += s;
                if (end) {
                    assertEquals(f3, read_f3);
                } else {
                    assertNotEquals(f3, read_f3);
                }
            } else if (index == 4000) {
                if (read_f4 == null) {
                    read_f4 = "";
                }
                read_f4 += s;
                if (end) {
                    assertEquals(f4, read_f4);
                } else {
                    assertNotEquals(f4, read_f4);
                }
            } else {
                fail();
            }
            return true;
        }

        @Override
        public boolean readFixedInt(int index, int value) {
            assertEquals(5, index);
            read_f5 = value;
            return true;
        }

        @Override
        public boolean readFixedLong(int index, long value) {
            assertEquals(6, index);
            read_f6 = value;
            return true;
        }
    }


    @Test
    public void testParse() {
        PbParser parser = new PbParser(500);
        Callback callback = new Callback(100, 200, "msg", "body", 100, 200);
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        callback = new Callback(Integer.MAX_VALUE, Long.MAX_VALUE, "msg", "body", Integer.MAX_VALUE, Long.MAX_VALUE);
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        callback = new Callback(-1, -1, "msg", "body", -1, -1);
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        callback = new Callback(-1000, -2000, "msg", "body", -1000, -2000);
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        callback = new Callback(Integer.MAX_VALUE, Long.MAX_VALUE, "msg", "body", Integer.MAX_VALUE, Long.MAX_VALUE);
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        try {
            callback = new Callback(1, 2, "msg", "body", 1, 2);
            new PbParser(5).parse(callback.buildFrame(), callback);
            fail();
        } catch (PbException e) {
        }
        assertEquals(0, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);
    }

    @Test
    public void testHalfParse() {
        int steps[] = new int[]{1, 2, 5, 9};
        for(int step: steps) {
            testHalfParse0(5, step, 0, 0, "1", "2", 0, 0);
            testHalfParse0(5, step, 1, 1, "1", "2", 1, 1);
            testHalfParse0(5, step, -1, -1, "1", "2", -1, -1);
            testHalfParse0(5, step, 1000, 2000, "1", "2", 1000, 2000);
            testHalfParse0(5, step, -1000, -2000, "1", "2", -1000, -2000);

            testHalfParse0(5, step, Integer.MAX_VALUE, Long.MAX_VALUE, "123", "234",
                    Integer.MAX_VALUE, Long.MAX_VALUE);

            testHalfParse0(5, step, Integer.MIN_VALUE, Long.MIN_VALUE, "123", "234",
                    Integer.MIN_VALUE, Long.MIN_VALUE);

            char[] msg = new char[257];
            Arrays.fill(msg, 'a');
            testHalfParse0(5, 20,1000, 2000, new String(msg), "2000", -1000, -2000);
        }
    }

    private void testHalfParse0(int loop, int maxStep, int f1, long f2, String f3, String f4, int f5, long f6) {
        PbParser parser = new PbParser(f3.length() + f4.length() + 100);
        Random r = new Random();
        for (int i = 0; i < loop; i++) {
            ArrayList<Integer> steps = new ArrayList<>();
            Callback callback = new Callback(f1, f2, f3, f4, f5, f6);
            byte[] fullBuffer = callback.buildFrame().array();
            try {
                int len = fullBuffer.length;
                for (int j = 0; j < len; ) {
                    int c = r.nextInt(maxStep) + 1;
                    ByteBuffer buf = ByteBuffer.allocate(c);
                    buf.order(ByteOrder.LITTLE_ENDIAN);
                    int readCount = Math.min(c, len - j);
                    steps.add(readCount);
                    buf.put(fullBuffer, j, readCount);
                    buf.flip();
                    parser.parse(buf, callback);
                    j += readCount;
                }
                assertEquals(1, callback.beginCount);
                assertEquals(1, callback.endSuccessCount);
                assertEquals(0, callback.endFailCount);
            } catch (Throwable e) {
                System.out.println("fail. i=" + i + ",steps=" + steps);
                throw e;
            }
        }
    }

    @Test
    public void testCallbackFail() {
        PbParser parser = new PbParser(500);

        Callback callback = new Callback(10000, 20000, "msg", "body", 10000, 20000) {
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
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);

        callback = new Callback(10000, 20000, "msg", "body", 10000, 20000) {
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
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);

        callback = new Callback(10000, 20000, "msg", "body", 10000, 20000) {
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
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);

        callback = new Callback(10000, 20000, "msg", "body", 10000, 20000) {
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
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);

        callback = new Callback(10000, 20000, "msg", "body", 10000, 20000) {
            @Override
            public boolean readFixedInt(int index, int value) {
                return false;
            }
        };
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);

        callback = new Callback(10000, 20000, "msg", "body", 10000, 20000) {
            @Override
            public boolean readFixedInt(int index, int value) {
                throw new ArrayIndexOutOfBoundsException();
            }
        };
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(1, callback.endFailCount);

        callback = new Callback(100, 200, "msg", "body", 100, 200);
        parser.parse(callback.buildFrame(), callback);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);
    }
}
