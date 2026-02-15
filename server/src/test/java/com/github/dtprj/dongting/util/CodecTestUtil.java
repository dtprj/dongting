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
package com.github.dtprj.dongting.util;

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbParser;
import com.github.dtprj.dongting.codec.SimpleEncodable;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.raft.impl.DecodeContextEx;
import org.junit.jupiter.api.Assertions;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class CodecTestUtil {

    private final static ByteBufferPool pool = new DefaultPoolFactory().createPool(new Timestamp(), false);
    private final static RefBufferFactory refBufferFactory = new RefBufferFactory(pool, 128);

    public static DecodeContextEx decodeContext() {
        DecodeContextEx c = new DecodeContextEx();
        c.setHeapPool(refBufferFactory);
        return c;
    }

    public static EncodeContext encodeContext() {
        return new EncodeContext(refBufferFactory);
    }

    public static ByteBuffer fullBufferEncode(Encodable e) {
        ByteBuffer buf = ByteBuffer.allocate(512);
        int size = e.actualSize();
        EncodeContext encodeContext = encodeContext();
        Assertions.assertTrue(e.encode(encodeContext, buf));
        buf.flip();
        Assertions.assertEquals(size, buf.remaining());
        return buf;
    }

    public static <T> T fullBufferDecode(ByteBuffer buf, PbCallback<T> callback) {
        buf.position(0);
        PbParser p = new PbParser();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, buf.limit());
        Object o = p.parse(buf);
        Assertions.assertNotNull(o);
        //noinspection unchecked
        return (T) o;
    }

    public static Object smallBufferEncodeAndParse(Encodable e, PbCallback<?> callback) {
        EncodeContext c = CodecTestUtil.encodeContext();
        int size = e.actualSize();
        PbParser p = new PbParser();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, e.actualSize());

        ByteBuffer smallBuf = ByteBuffer.allocate(1);
        ByteBuffer buf = smallBuf;
        int encodedSize = 0;
        boolean encodeFinished;
        Object o = null;
        do {
            encodeFinished = e.encode(c, buf);
            if (buf.position() == 0) {
                buf = ByteBuffer.allocate(buf.capacity() + 1);
                continue;
            }
            buf.flip();
            encodedSize += buf.remaining();
            o = p.parse(buf);
            buf = smallBuf;
            buf.clear();
        } while (!encodeFinished);

        Assertions.assertEquals(size, encodedSize);
        Assertions.assertNotNull(o);
        return o;
    }

    public static ByteBuffer simpleEncode(SimpleEncodable e) {
        ByteBuffer buf = ByteBuffer.allocate(e.actualSize());
        e.encode(buf);
        buf.flip();
        return buf;
    }
}
