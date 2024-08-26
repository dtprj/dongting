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
public class Decoder {
    private boolean beginCalled = false;
    private boolean endCalled = false;
    private boolean skip = false;

    private DecodeContext context;
    private DecoderCallback<?> callback;

    public Decoder(DecodeContext context, DecoderCallback<?> callback) {
        this.context = context;
        this.callback = callback;
    }

    public Decoder() {
    }

    void reset() {
        if (beginCalled && !endCalled) {
            callEndAndReset(false);
        }
    }

    private Object callEndAndReset(boolean success) {
        if (endCalled) {
            return null;
        }
        endCalled = true;
        try {
            if (success) {
                success = callback.end(true);
            } else {
                callback.end(false);
            }
            if (success) {
                return callback.getResult();
            } else {
                return null;
            }
        } finally {
            context.status = null;
            callback.context = null;
            skip = !success;
        }
    }

    public boolean isFinished() {
        return endCalled;
    }

    public boolean shouldSkip() {
        return skip;
    }

    public void prepareNext(DecodeContext context, DecoderCallback<?> callback) {
        this.context = context;
        this.callback = callback;

        callback.context = context;

        this.beginCalled = false;
        this.endCalled = false;
        this.skip = false;
    }

    public final Object decode(ByteBuffer buffer, int bodyLen, int currentPos) {
        if (endCalled) {
            throw new CodecException("decode finished");
        }
        if (currentPos == 0 && !beginCalled) {
            beginCalled = true;
            callback.begin(bodyLen);
        }
        if (bodyLen == 0) {
            return callEndAndReset(true);
        }
        int oldPos = buffer.position();
        int oldLimit = buffer.limit();
        int endPos = Math.min(oldLimit, oldPos - currentPos + bodyLen);
        if (skip) {
            buffer.position(endPos);
        } else {
            if (oldLimit > endPos) {
                buffer.limit(endPos);
            }
            try {
                skip = !callback.doDecode(buffer, bodyLen, currentPos);
            } catch (RuntimeException | Error e) {
                skip = true;
                callEndAndReset(false);
                throw e;
            } finally {
                buffer.limit(oldLimit);
                buffer.position(endPos);
            }
        }
        if (oldLimit >= endPos) {
            return callEndAndReset(!skip);
        }
        return null;
    }

}
