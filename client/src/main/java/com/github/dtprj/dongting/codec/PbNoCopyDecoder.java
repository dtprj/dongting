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
@SuppressWarnings("Convert2Diamond")
public final class PbNoCopyDecoder<T> extends Decoder<T> {

    private final Function<DecodeContext, PbCallback<T>> callbackCreator;

    public PbNoCopyDecoder(Function<DecodeContext, PbCallback<T>> callbackCreator) {
        this.callbackCreator = callbackCreator;
    }

    @Override
    public T doDecode(DecodeContext context, ByteBuffer buffer, int bodyLen, int currentPos) {
        PbCallback<T> callback;
        PbParser p = context.nestedParser;
        if (currentPos == 0) {
            callback = callbackCreator.apply(context);
            if (p == null) {
                p = new PbParser(callback, bodyLen);
                context.nestedParser = p;
            } else {
                p.prepareNext(callback, bodyLen);
            }
        } else {
            //noinspection unchecked
            callback = (PbCallback<T>) p.callback;
        }
        boolean end = buffer.remaining() >= bodyLen - currentPos;
        p.parse(buffer);

        if (end) {
            if (!p.checkSingleEndStatus()) {
                throw new PbException("parse not finish after read all bytes. bodyLen="
                        + bodyLen + ", currentPos=" + currentPos + "class=" + getClass());
            }
            return callback.getResult();
        } else {
            if (!p.checkNotSingleEndStatus()) {
                throw new PbException("parse finished without read all bytes. bodyLen="
                        + bodyLen + ", currentPos=" + currentPos + "class=" + getClass());
            }
            return null;
        }
    }

    @Override
    public void finish(DecodeContext context) {
        PbParser p = context.nestedParser;
        if (p != null) {
            p.reset();
        }
    }

    public static final PbNoCopyDecoder<Integer> SIMPLE_INT_DECODER = new PbNoCopyDecoder<Integer>(c -> new PbCallback<Integer>() {
        private int value;

        @Override
        public boolean readFix32(int index, int value) {
            if (index == 1) {
                this.value = value;
            }
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
            if (index == 1) {
                this.value = value;
            }
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
            if (index == 1) {
                value = parseUTF8(buf, fieldLen, currentPos);
            }
            return true;
        }

        @Override
        public String getResult() {
            return value;
        }
    });

}
