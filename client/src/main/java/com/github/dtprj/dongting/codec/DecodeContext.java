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
import com.github.dtprj.dongting.dtkv.KvResp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

/**
 * @author huangli
 */
public class DecodeContext {
    private static final DtLog log = DtLogs.getLogger(DecodeContext.class);

    static final int THREAD_LOCAL_BUFFER_SIZE = 4 * 1024;
    private static final ThreadLocal<byte[]> THREAD_LOCAL_BUFFER = ThreadLocal.withInitial(() -> new byte[THREAD_LOCAL_BUFFER_SIZE]);
    private final byte[] threadLocalBuffer = THREAD_LOCAL_BUFFER.get();

    private RefBufferFactory heapPool;

    private PbParser nestedParser;
    private Decoder nestedDecoder;
    private DecodeContext nestedContext;

    // reset in PbParser and Decoder
    Object status;

    // caches
    private PbNoCopyDecoderCallback pbNoCopyDecoderCallback;
    private KvResp.Callback kvRespCallback;
    private RefBufferDecoderCallback refBufferDecoderCallback;
    private RefBufferDecoderCallback refBufferDecoderCallbackPlain;

    public DecodeContext() {
    }

    protected DecodeContext createNestedInstance() {
        return new DecodeContext();
    }

    public void reset(PbParser root) {
        reset();
        try {
            root.reset();
        } catch (Exception e) {
            log.error("reset error", e);
        }
    }

    public void reset(Decoder root) {
        reset();
        try {
            root.reset();
        } catch (Exception e) {
            log.error("reset error", e);
        }
    }

    private void reset() {
        if (nestedContext != null) {
            nestedContext.reset();
        }
        if (nestedParser != null) {
            nestedParser.reset();
        }
        if (nestedDecoder != null) {
            nestedDecoder.reset();
        }
    }

    public DecodeContext createOrGetNestedContext() {
        if (nestedContext == null) {
            nestedContext = createNestedInstance();
            nestedContext.heapPool = heapPool;
        }
        return nestedContext;
    }

    public PbParser createOrGetNestedParser() {
        if (nestedParser == null) {
            nestedParser = new PbParser();
        }
        return nestedParser;
    }

    public Decoder createOrGetNestedDecoder() {
        if (nestedDecoder == null) {
            nestedDecoder = new Decoder();
        }
        return nestedDecoder;
    }

    public RefBufferFactory getHeapPool() {
        return heapPool;
    }

    public void setHeapPool(RefBufferFactory heapPool) {
        this.heapPool = heapPool;
    }

    public byte[] getThreadLocalBuffer() {
        return threadLocalBuffer;
    }

    @SuppressWarnings("unchecked")
    public <T> DecoderCallback<T> toDecoderCallback(PbCallback<T> callback) {
        PbNoCopyDecoderCallback c = pbNoCopyDecoderCallback;
        if (c == null) {
            c = new PbNoCopyDecoderCallback();
            this.pbNoCopyDecoderCallback = c;
        }
        c.prepareNext(callback);
        return (DecoderCallback<T>) c;
    }

    public RefBufferDecoderCallback refBufferDecoderCallback(boolean plain) {
        RefBufferDecoderCallback c;
        if (plain) {
            c = refBufferDecoderCallbackPlain;
            if (c == null) {
                c = new RefBufferDecoderCallback(true);
                this.refBufferDecoderCallbackPlain = c;
            }
        } else {
            c = refBufferDecoderCallback;
            if (c == null) {
                c = new RefBufferDecoderCallback(false);
                this.refBufferDecoderCallback = c;
            }
        }
        return c;
    }

    public KvResp.Callback kvRespCallback() {
        if (kvRespCallback == null) {
            kvRespCallback = new KvResp.Callback();
        }
        return kvRespCallback;
    }
}
