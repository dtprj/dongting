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
import java.util.function.Supplier;

/**
 * @author huangli
 */
public final class PbNoCopyDecoderCallback<T> extends DecoderCallback<T> {

    private final Supplier<PbCallback<T>> callbackCreator;
    private PbCallback<T> pbCallback;
    private Object result;

    public PbNoCopyDecoderCallback(Supplier<PbCallback<T>> callbackCreator) {
        this.callbackCreator = callbackCreator;
    }

    @Override
    protected boolean end(boolean success) {
        pbCallback = null;
        result = null;
        return success;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T getResult() {
        return (T) result;
    }

    @Override
    public boolean doDecode(ByteBuffer buffer, int bodyLen, int currentPos) {
        PbParser p;
        if (currentPos == 0) {
            pbCallback = callbackCreator.get();
            p = context.prepareNestedParser(pbCallback, bodyLen);
        } else {
            p = context.nestedParser;
        }
        boolean end = buffer.remaining() >= bodyLen - currentPos;
        result = p.parse(buffer);

        if (end) {
            if (!p.isFinished()) {
                throw new PbException("parse not finish after read all bytes. bodyLen="
                        + bodyLen + ", currentPos=" + currentPos + ", callback=" + pbCallback);
            }
        } else {
            if (p.isFinished()) {
                throw new PbException("parse finished without read all bytes. bodyLen="
                        + bodyLen + ", currentPos=" + currentPos + ", callback=" + pbCallback);
            }
        }
        return true;
    }

    public static final class IntCallback extends PbCallback<Integer> {

        @Override
        public boolean readFix32(int index, int value) {
            if (index == 1) {
                this.context.status = value;
            }
            return true;
        }

        @Override
        protected Integer getResult() {
            Integer r = (Integer) this.context.status;
            return r == null ? 0 : r;
        }
    }

    public static final class LongCallback extends PbCallback<Long> {

        @Override
        public boolean readFix64(int index, long value) {
            if (index == 1) {
                this.context.status = value;
            }
            return true;
        }

        @Override
        protected Long getResult() {
            Long r = (Long) this.context.status;
            return r == null ? 0 : r;
        }
    }

    public static final class StringCallback extends PbCallback<String> {

        private String s;

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == 1) {
                s = parseUTF8(buf, fieldLen, currentPos);
            }
            return true;
        }

        @Override
        protected String getResult() {
            return s;
        }

        @Override
        protected boolean end(boolean success) {
            s = null;
            return success;
        }
    }

}
