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
package com.github.dtprj.dongting.common;

import com.github.dtprj.dongting.buf.Buffers;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.PbParser;

/**
 * @author huangli
 */
public class DtThread extends Thread {
    public static final int THREAD_LOCAL_BUFFER_SIZE = 4 * 1024;
    
    public final byte[] threadLocalBuffer = new byte[THREAD_LOCAL_BUFFER_SIZE];

    public final DecodeContext decodeContext;
    public final PbParser parser = new PbParser();
    public final Buffers buffers;
    
    protected DtThread(Runnable r, String name, Buffers pool) {
        super(r, name);
        this.decodeContext = DecodeContext.factory.apply(pool, threadLocalBuffer);
        this.buffers = pool;
    }
    
    public static DtThread currentDtThread() {
        Thread current = Thread.currentThread();
        if (current instanceof DtThread) {
            return (DtThread) current;
        }
        throw new DtException("not in DtThread");
    }
}
