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

import com.github.dtprj.dongting.common.DtException;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
public class PbParserTest {

    static class Msg {
        private int f1;
        private long f2;
        private String f3;
        private String f4;
        private int f5;
        private long f6;

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Msg msg = (Msg) o;
            return f1 == msg.f1 && f2 == msg.f2 && f5 == msg.f5 && f6 == msg.f6 && Objects.equals(f3, msg.f3)
                    && Objects.equals(f4, msg.f4);
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    static class Callback extends PbCallback {

        private Msg msg;
        private Msg readMsg;

        private int beginCount;
        private int endSuccessCount;
        private int endFailCount;

        private int expectLen;

        private int setF1Count;
        private int setF2Count;
        private int f3BeginCount;
        private int f3EndCount;
        private int f4BeginCount;
        private int f4EndCount;
        private int setF5Count;
        private int setF6Count;

        PbParser parser;

        Callback(int f1, long f2, String f3, String f4, int f5, long f6) {
            msg = new Msg();
            msg.f1 = f1;
            msg.f2 = f2;
            msg.f3 = f3;
            msg.f4 = f4;
            msg.f5 = f5;
            msg.f6 = f6;
        }

        public void reset(int f1, long f2, String f3, String f4, int f5, long f6) {
            msg.f1 = f1;
            msg.f2 = f2;
            msg.f3 = f3;
            msg.f4 = f4;
            msg.f5 = f5;
            msg.f6 = f6;

            this.expectLen = 0;
            this.beginCount = 0;
            this.endSuccessCount = 0;
            this.endFailCount = 0;

            readMsg = null;

            setF1Count = 0;
            setF2Count = 0;
            f3BeginCount = 0;
            f3EndCount = 0;
            f4BeginCount = 0;
            f4EndCount = 0;
            setF5Count = 0;
            setF6Count = 0;
        }

        public ByteBuffer buildFrame() {
            return buildFrame(true);
        }

        public ByteBuffer buildFrame(boolean hasLenField) {
            DtPbTest.PbParserTestMsg.Builder builder = DtPbTest.PbParserTestMsg.newBuilder()
                    .setInt32Field(msg.f1)
                    .setInt64Field(msg.f2)
                    .setStringField(msg.f3)
                    .setBytesField(ByteString.copyFrom(msg.f4.getBytes()))
                    .setInt32Fix(msg.f5)
                    .setInt64Fix(msg.f6);
            byte[] bs = builder.build().toByteArray();
            ByteBuffer buf;
            if (hasLenField) {
                buf = ByteBuffer.allocate(bs.length + 4);
                buf.putInt(bs.length);
            } else {
                buf = ByteBuffer.allocate(bs.length);
            }

            buf.put(bs);
            buf.flip();
            this.expectLen = bs.length;
            buf.order(ByteOrder.LITTLE_ENDIAN);
            return buf;
        }

        @Override
        public void begin(int len, PbParser parser) {
            this.parser = parser;
            beginCount++;
            assertEquals(expectLen, len);
            readMsg = new Msg();
            readMsg.f3 = "";
            readMsg.f4 = "";

            setF1Count = 0;
            setF2Count = 0;
            f3BeginCount = 0;
            f3EndCount = 0;
            f4BeginCount = 0;
            f4EndCount = 0;
            setF5Count = 0;
            setF6Count = 0;
        }

        @Override
        public void end(boolean success) {
            if (success) {
                endSuccessCount++;
                try {
                    assertEquals(msg, readMsg);

                    assertEquals(msg.f1 == 0 ? 0 : 1, setF1Count);
                    assertEquals(msg.f2 == 0 ? 0 : 1, setF2Count);

                    assertEquals(msg.f3.length() == 0 ? 0 : 1, f3BeginCount);
                    assertEquals(msg.f3.length() == 0 ? 0 : 1, f3EndCount);

                    assertEquals(msg.f4.length() == 0 ? 0 : 1, f4BeginCount);
                    assertEquals(msg.f4.length() == 0 ? 0 : 1, f4EndCount);

                    assertEquals(msg.f5 == 0 ? 0 : 1, setF5Count);
                    assertEquals(msg.f6 == 0 ? 0 : 1, setF6Count);
                } catch (Throwable e) {
                    e.printStackTrace();
                    endFailCount++;
                }
            } else {
                endFailCount++;
            }
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == 1) {
                readMsg.f1 = (int) value;
                setF1Count++;
            } else if (index == 2) {
                readMsg.f2 = value;
                setF2Count++;
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
                readMsg.f3 += s;
                f3BeginCount += begin ? 1 : 0;
                f3EndCount += end ? 1 : 0;
            } else if (index == 4000) {
                readMsg.f4 += s;
                f4BeginCount += begin ? 1 : 0;
                f4EndCount += end ? 1 : 0;
            } else if (index == 7) {
//                PbParser nc;
//                if (begin) {
//                    nc = parser.createOrGetNestedParserSingle(f7, len);
//                } else {
//                    nc = parser.getNestedParser();
//                }
//                nc.parse(buf);
            } else {
                fail();
            }
            return true;
        }

        @Override
        public boolean readFix32(int index, int value) {
            assertEquals(5, index);
            readMsg.f5 = value;
            setF5Count++;
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            assertEquals(6, index);
            readMsg.f6 = value;
            setF6Count++;
            return true;
        }
    }


    @Test
    public void testParse() {
        Callback callback = new Callback(0, 0, "", "", 0, 0);
        PbParser parser = PbParser.multiParser(callback, 500);
        parser.parse(callback.buildFrame());
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        callback = new Callback(100, 200, "msg", "body", 100, 200);
        parser = PbParser.multiParser(callback, 500);
        parser.parse(callback.buildFrame());
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        callback.reset(Integer.MAX_VALUE, Long.MAX_VALUE, "msg", "body", Integer.MAX_VALUE, Long.MAX_VALUE);
        parser.parse(callback.buildFrame());
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        callback.reset(-1, -1, "msg", "body", -1, -1);
        parser.parse(callback.buildFrame());
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        callback.reset(-1000, -2000, "msg", "body", -1000, -2000);
        parser.parse(callback.buildFrame());
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        callback.reset(Integer.MAX_VALUE, Long.MAX_VALUE, "msg", "body", Integer.MAX_VALUE, Long.MAX_VALUE);
        parser.parse(callback.buildFrame());
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        try {
            callback = new Callback(1, 2, "msg", "body", 1, 2);
            PbParser.multiParser(callback, 5).parse(callback.buildFrame());
            fail();
        } catch (PbException e) {
            // ignore
        }
        assertEquals(0, callback.beginCount);
        assertEquals(0, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);
    }

    @Test
    public void testSingleParse() {
        Callback callback = new Callback(100, 200, "msg", "body", 100, 200);
        ByteBuffer buffer = callback.buildFrame(false);
        PbParser parser = PbParser.singleParser(callback, buffer.remaining());
        parser.parse(buffer);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);

        callback.reset(100, 200, "msg", "body", 100, 200);
        assertThrows(DtException.class, () -> parser.parse(callback.buildFrame(false)));

        callback.reset(100, 200, "msg", "body", 100, 200);
        buffer = callback.buildFrame(false);
        parser.resetSingle(callback, buffer.remaining());
        parser.parse(buffer);
        assertEquals(1, callback.beginCount);
        assertEquals(1, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);
    }

    @Test
    public void testHalfParse() {
        int[] steps = new int[]{1, 2, 5, 9};
        for(int step: steps) {
            testHalfParse0(step, 0, 0, "1", "2", 0, 0);
            testHalfParse0(step, 1, 1, "1", "2", 1, 1);
            testHalfParse0(step, -1, -1, "1", "2", -1, -1);
            testHalfParse0(step, 1000, 2000, "1", "2", 1000, 2000);
            testHalfParse0(step, -1000, -2000, "1", "2", -1000, -2000);

            testHalfParse0(step, Integer.MAX_VALUE, Long.MAX_VALUE, "123", "234",
                    Integer.MAX_VALUE, Long.MAX_VALUE);

            testHalfParse0(step, Integer.MIN_VALUE, Long.MIN_VALUE, "123", "234",
                    Integer.MIN_VALUE, Long.MIN_VALUE);

            char[] msg = new char[257];
            Arrays.fill(msg, 'a');
            testHalfParse0(20,1000, 2000, new String(msg), "2000", -1000, -2000);
        }
    }

    private void testHalfParse0(int maxStep, int f1, long f2, String f3, String f4, int f5, long f6) {
        int loop = 10;
        Callback callback = new Callback(f1, f2, f3, f4, f5, f6);
        PbParser parser = PbParser.multiParser(callback, f3.length() + f4.length() + 100);
        byte[] fullBuffer = callback.buildFrame().array();
        ByteBuffer tempBuf = ByteBuffer.allocate(fullBuffer.length * loop);
        for (int i = 0; i < loop; i++) {
            tempBuf.put(fullBuffer);
        }
        fullBuffer = tempBuf.array();
        halfParse(maxStep, parser, fullBuffer);

        assertEquals(loop, callback.beginCount);
        assertEquals(loop, callback.endSuccessCount);
        assertEquals(0, callback.endFailCount);
    }

    private static void halfParse(int maxStep, PbParser parser, byte[] fullBuffer) {
        Random r = new Random();
        int len = fullBuffer.length;
        for (int j = 0; j < len; ) {
            int c = r.nextInt(maxStep + 1);
            ByteBuffer buf = ByteBuffer.allocate(c);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            int readCount = Math.min(c, len - j);
            buf.put(fullBuffer, j, readCount);
            buf.flip();
            parser.parse(buf);
            j += readCount;
        }
    }

    @Test
    public void testCallbackFail() {

        Supplier<Callback> supplier = () -> new Callback(10000, 20000, "msg", "body", 10000, 20000) {
            @Override
            public boolean readVarNumber(int index, long value) {
                if (index == 2) {
                    return false;
                }
                return super.readVarNumber(index, value);
            }
        };
        testCallbackFail0(supplier, 1, 0, 1);

        supplier = () -> new Callback(10000, 20000, "msg", "body", 10000, 20000) {
            @Override
            public boolean readVarNumber(int index, long value) {
                if (index == 2) {
                    throw new ArrayIndexOutOfBoundsException();
                }
                return super.readVarNumber(index, value);
            }
        };
        testCallbackFail0(supplier, 1, 0, 1);

        supplier = () -> new Callback(10000, 20000, "msg", "body", 10000, 20000) {
            @Override
            public boolean readBytes(int index, ByteBuffer buf, int len, boolean begin, boolean end) {
                if (index == 3) {
                    return false;
                }
                return super.readBytes(index, buf, len, begin, end);
            }
        };
        testCallbackFail0(supplier, 1, 0, 1);

        supplier = () -> new Callback(10000, 20000, "msg", "body", 10000, 20000) {
            @Override
            public boolean readBytes(int index, ByteBuffer buf, int len, boolean begin, boolean end) {
                if (index == 4000) {
                    throw new ArrayIndexOutOfBoundsException();
                }
                return super.readBytes(index, buf, len, begin, end);
            }
        };
        testCallbackFail0(supplier, 1, 0, 1);

        supplier = () -> new Callback(10000, 20000, "msg", "body", 10000, 20000) {
            @Override
            public boolean readFix32(int index, int value) {
                return false;
            }
        };
        testCallbackFail0(supplier, 1, 0, 1);

        supplier = () -> new Callback(10000, 20000, "msg", "body", 10000, 20000) {
            @Override
            public boolean readFix32(int index, int value) {
                throw new ArrayIndexOutOfBoundsException();
            }
        };
        testCallbackFail0(supplier, 1, 0, 1);

        supplier = () -> new Callback(10000, 20000, "msg", "body", 10000, 20000) {
            @Override
            public void begin(int len, PbParser p) {
                throw new ArrayIndexOutOfBoundsException();
            }
        };
        testCallbackFail0(supplier, 0, 0, 1);

        supplier = () -> new Callback(10000, 20000, "msg", "body", 10000, 20000) {
            @Override
            public void end(boolean success) {
                throw new ArrayIndexOutOfBoundsException();
            }
        };
        // since we override end(), the endFailCount not set
        testCallbackFail0(supplier, 1, 0, 0);

        supplier = () -> new Callback(100, 200, "msg", "body", 100, 200);
        testCallbackFail0(supplier, 1, 1, 0);
    }

    private void testCallbackFail0(Supplier<Callback> callbackBuilder, int expectBegin,
                                   int expectEndSuccess, int expectEndFail) {
        Callback callback = callbackBuilder.get();
        PbParser parser = PbParser.multiParser(callback, 500);

        parser.parse(callback.buildFrame());
        assertEquals(expectBegin, callback.beginCount);
        assertEquals(expectEndSuccess, callback.endSuccessCount);
        assertEquals(expectEndFail, callback.endFailCount);

        halfParse(1, parser, callback.buildFrame().array());
        halfParse(2, parser, callback.buildFrame().array());
        halfParse(3, parser, callback.buildFrame().array());
        halfParse(4, parser, callback.buildFrame().array());
        halfParse(5, parser, callback.buildFrame().array());
        halfParse(6, parser, callback.buildFrame().array());
        halfParse(7, parser, callback.buildFrame().array());
        halfParse(8, parser, callback.buildFrame().array());
        halfParse(9, parser, callback.buildFrame().array());

        assertEquals(expectBegin * 10, callback.beginCount);
        assertEquals(expectEndSuccess * 10, callback.endSuccessCount);
        assertEquals(expectEndFail * 10, callback.endFailCount);
    }
}
