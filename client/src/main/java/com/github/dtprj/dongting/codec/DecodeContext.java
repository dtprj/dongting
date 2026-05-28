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
import com.github.dtprj.dongting.dtkv.KvReq;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.function.BiFunction;

/**
 * Require decode in DtThread.
 *
 * @author huangli
 */
public class DecodeContext {
    private static final DtLog log = DtLogs.getLogger(DecodeContext.class);

    public final Buffers buffers;
    public final byte[] threadLocalBuffer;

    private PbParser nestedParser;
    private Decoder nestedDecoder;
    private DecodeContext nestedContext;

    // reset in PbParser and Decoder
    public Object status;

    // caches
    private PbNoCopyDecoderCallback pbNoCopyDecoderCallback;
    private PbIntCallback pbIntCallback;
    private PbLongCallback pbLongCallback;
    private PbStrCallback pbStrCallback;
    private PbBytesCallback pbBytesCallback;
    private KvReq.KvReqCallback kvReqCallback;

    // replace in server side to create DecodeContextEx
    public static BiFunction<Buffers, byte[], DecodeContext> factory = DecodeContext::new;

    protected DecodeContext(Buffers f, byte[] b) {
        this.buffers = f;
        this.threadLocalBuffer = b;
    }

    protected DecodeContext createNestedInstance() {
        return new DecodeContext(buffers, threadLocalBuffer);
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

    public PbIntCallback cachedPbIntCallback() {
        if (pbIntCallback == null) {
            pbIntCallback = new PbIntCallback();
        }
        return pbIntCallback;
    }

    public PbLongCallback cachedPbLongCallback() {
        if (pbLongCallback == null) {
            pbLongCallback = new PbLongCallback();
        }
        return pbLongCallback;
    }

    public PbStrCallback cachedPbStrCallback() {
        if (pbStrCallback == null) {
            pbStrCallback = new PbStrCallback();
        }
        return pbStrCallback;
    }

    public PbBytesCallback cachedPbBytesCallback() {
        if (pbBytesCallback == null) {
            pbBytesCallback = new PbBytesCallback();
        }
        return pbBytesCallback;
    }

    public KvReq.KvReqCallback kvReqCallback() {
        if (kvReqCallback == null) {
            kvReqCallback = new KvReq.KvReqCallback();
        }
        return kvReqCallback;
    }

}
