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

import com.github.dtprj.dongting.buf.RefBufferFactory;

/**
 * @author huangli
 */
public class DecodeContext {
    private static final int THREAD_LOCAL_BUFFER_SIZE = 4 * 1024;
    private static final ThreadLocal<byte[]> THREAD_LOCAL_BUFFER = ThreadLocal.withInitial(() -> new byte[THREAD_LOCAL_BUFFER_SIZE]);

    private RefBufferFactory heapPool;
    private Object status;
    private PbParser pbParser;

    private final byte[] threadLocalBuffer = THREAD_LOCAL_BUFFER.get();

    public DecodeContext() {
    }

    // only use by PbNoCopyDecoder
    PbParser createOrResetPbParser(PbCallback<?> callback, int len) {
        PbParser p = this.pbParser;
        if (p == null) {
            p = PbParser.singleParser(callback, len);
            this.pbParser = p;
            return p;
        } else {
            p.resetSingle(callback, len);
            return p;
        }
    }

    public void reset() {
        if (pbParser != null) {
            pbParser.reset();
        }
        status = null;
    }

    // only use by PbNoCopyDecoder
    PbParser getPbParser() {
        return pbParser;
    }

    public RefBufferFactory getHeapPool() {
        return heapPool;
    }

    public void setHeapPool(RefBufferFactory heapPool) {
        this.heapPool = heapPool;
    }

    public Object getStatus() {
        return status;
    }

    public void setStatus(Object status) {
        this.status = status;
    }

    public byte[] getThreadLocalBuffer() {
        return threadLocalBuffer;
    }
}
