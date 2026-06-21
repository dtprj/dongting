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
import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.buf.PoolFactory;
import com.github.dtprj.dongting.common.DtThread;
import com.github.dtprj.dongting.common.Timestamp;

/**
 * @author huangli
 */
public class CodecTestUtil {
    // single-threaded: init threadSafeRelease* pools to point at the owner thread,
    // so borrow(..., threadSafeRelease=true, ...) does not NPE. The cross-thread
    // callback is never invoked because tests always run in the owner thread.
    private static final Buffers buffer = createBuffers();

    private static Buffers createBuffers() {
        PoolFactory factory = new DefaultPoolFactory();
        Buffers buffers = factory.createPool(new Timestamp());
        factory.initPool(buffers, Thread.currentThread(),
                (rb, c) -> { c.accept(rb); return true; });
        return buffers;
    }

    // should test in single thread
    public static DecodeContext createContext() {
        return DecodeContext.factory.apply(buffer, new byte[DtThread.THREAD_LOCAL_BUFFER_SIZE]);
    }

    // should test in single thread
    public static EncodeContext createEncodeContext() {
        return new EncodeContext(buffer);
    }
}
