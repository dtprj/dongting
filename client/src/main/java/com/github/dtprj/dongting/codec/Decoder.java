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

    private DecodeContext context;
    private DecoderCallback<?> callback;

    public Decoder() {
    }

    void reset() {
        if (beginCalled && !endCalled) {
            callEndAndReset(false);
        }
    }

    private void callEndAndReset(boolean success) {
        if (endCalled) {
            return;
        }
        endCalled = true;
        try {
            callback.end(success);
        } finally {
            context.status = null;
            callback.context = null;
        }
    }

    public boolean isFinished() {
        return endCalled;
    }

    public void prepareNext(DecodeContext context, DecoderCallback<?> callback) {
        this.context = context;
        this.callback = callback;

        context.status = null;
        callback.context = context;

        this.beginCalled = false;
        this.endCalled = false;
    }

    public final Object decode(ByteBuffer buffer, int bodyLen, int currentPos) {
        if (endCalled) {
            throw new CodecException("decode finished");
        }
        if (currentPos == 0 && !beginCalled) {
            beginCalled = true;
            callback.begin(bodyLen);
        }
        int oldLimit = buffer.limit();
        int recordEndPos = buffer.position() + bodyLen - currentPos;
        boolean allDataAvailable = oldLimit >= recordEndPos;
        try {
            if (oldLimit > recordEndPos) {
                buffer.limit(recordEndPos);
            }
            callback.doDecode(buffer, bodyLen, currentPos);
            if (buffer.position() != (allDataAvailable ? recordEndPos : oldLimit)) {
                throw new CodecException("doDecode didn't consume all bytes. "
                        + "bodyLen=" + bodyLen + ", currentPos=" + currentPos
                        + ", class=" + callback.getClass().getName());
            }
            return allDataAvailable ? callback.getResult() : null;
        } catch (RuntimeException | Error e) {
            if (oldLimit > recordEndPos) {
                buffer.limit(oldLimit);
            }
            buffer.position(allDataAvailable ? recordEndPos : oldLimit);
            callEndAndReset(false);
            throw e;
        } finally {
            if (oldLimit > recordEndPos) {
                buffer.limit(oldLimit);
            }
            if (allDataAvailable) {
                callEndAndReset(true);
            }
        }
    }

}
