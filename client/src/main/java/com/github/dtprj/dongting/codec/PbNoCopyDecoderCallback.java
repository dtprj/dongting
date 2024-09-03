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

/**
 * @author huangli
 */
final class PbNoCopyDecoderCallback<T> extends DecoderCallback<T> {

    private Object result;
    private PbCallback<T> callback;

    PbNoCopyDecoderCallback() {
    }

    void prepareNext(PbCallback<T> callback) {
        this.result = null;
        this.callback = callback;
    }

    @Override
    protected boolean end(boolean success) {
        result = null;
        callback = null;
        return success;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T getResult() {
        return (T) result;
    }

    @Override
    public boolean doDecode(ByteBuffer buffer, int bodyLen, int currentPos) {
        PbParser p = context.getOrCreateNestedParser();
        if (currentPos == 0) {
            p.prepareNext(context.getOrCreateNestedContext(), callback, bodyLen);
        }
        boolean end = buffer.remaining() >= bodyLen - currentPos;
        result = p.parse(buffer);

        if (end) {
            if (!p.isFinished()) {
                throw new PbException("parse not finish after read all bytes. bodyLen="
                        + bodyLen + ", currentPos=" + currentPos + ", callback=" + p.callback);
            }
        } else {
            if (p.isFinished()) {
                throw new PbException("parse finished without read all bytes. bodyLen="
                        + bodyLen + ", currentPos=" + currentPos + ", callback=" + p.callback);
            }
        }
        return true;
    }

}
