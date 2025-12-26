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

import com.github.dtprj.dongting.common.MutableInt;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class PbNoCopyDecoderCallbackTest {

    @Test
    public void testSimple() {
        DecodeContext c = CodecTestUtil.createContext();
        Decoder decoder = new Decoder();
        decoder.prepareNext(c, c.toDecoderCallback(c.cachedPbIntCallback()));

        ByteBuffer buf = ByteBuffer.allocate(30);
        PbUtil.writeFix32Field(buf, 1, 2000);
        buf.flip();

        Object r = decoder.decode(buf, buf.remaining(), 0);
        assertEquals(2000, r);

        buf.clear();
        PbUtil.writeFix32Field(buf, 1, 0);
        buf.flip();

        decoder.prepareNext(c, c.toDecoderCallback(c.cachedPbIntCallback()));
        r = decoder.decode(buf, buf.remaining(), 0);
        assertEquals(0, r);
    }

    @Test
    public void testCancel() {
        DecodeContext c = CodecTestUtil.createContext();
        Decoder decoder = new Decoder();
        MutableInt count = new MutableInt(0);
        PbCallback<Object> callback = new PbCallback<>() {

            @Override
            public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
                count.increment();
                return false;
            }

            @Override
            protected Object getResult() {
                return null;
            }
        };
        decoder.prepareNext(c, c.toDecoderCallback(callback));
        ByteBuffer buf = ByteBuffer.allocate(30);
        PbUtil.writeAsciiField(buf, 1, "12313213213123");
        buf.flip();
        int limit = buf.limit();
        assertNull(decoder.decode(buf, limit, 0));
        assertEquals(1, count.getValue());
        assertTrue(decoder.isFinished());

        count.setValue(0);
        decoder.prepareNext(c, c.toDecoderCallback(callback));
        buf.position(0);
        buf.limit(5);
        assertNull(decoder.decode(buf, limit, 0));
        assertEquals(1, count.getValue());
        assertFalse(decoder.isFinished());
        assertTrue(decoder.shouldSkip());
        buf.position(5);
        buf.limit(limit);
        assertNull(decoder.decode(buf, limit, 5));
        assertEquals(1, count.getValue());
        assertTrue(decoder.isFinished());
    }
}
