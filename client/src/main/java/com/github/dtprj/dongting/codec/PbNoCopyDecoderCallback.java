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
final class PbNoCopyDecoderCallback extends DecoderCallback<Object> {

    private Object result;
    private PbCallback<?> callback;
    private final PbParser parser = new PbParser();

    PbNoCopyDecoderCallback() {
    }

    void prepareNext(PbCallback<?> callback) {
        this.result = null;
        this.callback = callback;
    }

    @Override
    protected boolean end(boolean success) {
        result = null;
        callback = null;
        parser.reset();
        return success;
    }

    @Override
    protected Object getResult() {
        return result;
    }

    @Override
    public boolean doDecode(ByteBuffer buffer, int bodyLen, int currentPos) {
        if (currentPos == 0) {
            parser.prepareNext(context, callback, bodyLen);
        }
        boolean end = buffer.remaining() >= bodyLen - currentPos;
        result = parser.parse(buffer);

        if (end) {
            if (!parser.isFinished()) {
                throw new PbException("parse not finish after read all bytes. bodyLen="
                        + bodyLen + ", currentPos=" + currentPos + ", callback=" + parser.callback);
            }
            return !parser.shouldSkip();
        } else {
            if (parser.isFinished()) {
                throw new PbException("parse finished without read all bytes. bodyLen="
                        + bodyLen + ", currentPos=" + currentPos + ", callback=" + parser.callback);
            }
            return true;
        }
    }

}
