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

import com.github.dtprj.dongting.buf.Buffers;
import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.common.DtThread;
import com.github.dtprj.dongting.common.Timestamp;

/**
 * @author huangli
 */
public class CodecTestUtil {
    private static final ByteBufferPool pool = new DefaultPoolFactory().createPool(new Timestamp(), false);
    private static final ByteBufferPool directPool = new DefaultPoolFactory().createPool(new Timestamp(), true);
    // should test in single thread
    private static final Buffers buffer = new Buffers(pool, directPool, pool, directPool);

    // should test in single thread
    public static DecodeContext createContext() {
        return DecodeContext.factory.apply(buffer, new byte[DtThread.THREAD_LOCAL_BUFFER_SIZE]);
    }

    // should test in single thread
    public static EncodeContext createEncodeContext() {
        return new EncodeContext(buffer);
    }
}
