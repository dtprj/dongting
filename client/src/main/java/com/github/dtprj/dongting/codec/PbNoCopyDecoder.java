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

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * @author huangli
 */
public final class PbNoCopyDecoder<T> implements Decoder<T> {

    private final Function<DecodeContext, PbCallback<T>> callbackCreator;

    public PbNoCopyDecoder(Function<DecodeContext, PbCallback<T>> callbackCreator) {
        this.callbackCreator = callbackCreator;
    }

    @Override
    public T decode(DecodeContext context, ByteBuffer buffer, int bodyLen, int currentPos) {
        PbParser parser;
        PbCallback<T> callback;
        if (currentPos == 0) {
            callback = callbackCreator.apply(context);
            parser = context.createOrResetPbParser(callback, bodyLen);
        } else {
            parser = context.getPbParser();
            callback = parser.getCallback();
        }
        boolean end = buffer.remaining() >= bodyLen - currentPos;
        parser.parse(buffer);
        if (end) {
            return callback.getResult();
        } else {
            return null;
        }
    }

    @Override
    public void finish(DecodeContext context) {
        PbParser parser = context.getPbParser();
        if (parser != null) {
            parser.finishParse();
        }
    }

    public static final PbNoCopyDecoder<Integer> SIMPLE_INT_DECODER = new PbNoCopyDecoder<Integer>(c -> new PbCallback<Integer>() {
        private int value;

        @Override
        public boolean readFix32(int index, int value) {
            this.value = value;
            return true;
        }

        @Override
        public Integer getResult() {
            return value;
        }
    });

    public static final PbNoCopyDecoder<Long> SIMPLE_LONG_DECODER = new PbNoCopyDecoder<Long>(c -> new PbCallback<Long>() {
        private long value;

        @Override
        public boolean readFix64(int index, long value) {
            this.value = value;
            return true;
        }

        @Override
        public Long getResult() {
            return value;
        }
    });

    public static final PbNoCopyDecoder<String> SIMPLE_STR_DECODER = new PbNoCopyDecoder<String>(c -> new PbCallback<String>() {
        private String value;

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            value = StrFiledDecoder.parseUTF8(c, buf, fieldLen, currentPos);
            return true;
        }

        @Override
        public String getResult() {
            return value;
        }

        @Override
        public void end(boolean success) {
            super.end(success);
            StrFiledDecoder.INSTANCE.finish(c);
        }
    });

}
