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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.codec.EncodeContext;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class ByteBufferWriteFrame extends WriteFrame {
    private ByteBuffer body;
    private int markedPosition = -1;

    public ByteBufferWriteFrame(ByteBuffer body) {
        this.body = body;
    }

    @Override
    protected int calcActualBodySize(EncodeContext context) {
        ByteBuffer body = this.body;
        return body == null ? 0 : body.remaining();
    }

    public static int copy(ByteBuffer src, ByteBuffer dest, int markedPosition) {
        int srcStart = src.position();
        if (markedPosition < 0) {
            markedPosition = srcStart;
        } else {
            src.position(markedPosition);
        }
        int destRemaining = dest.remaining();
        if (src.remaining() > destRemaining) {
            int srcLimit = src.limit();
            src.limit(markedPosition + destRemaining);
            dest.put(src);

            src.limit(srcLimit);
            src.position(srcStart);
            return markedPosition + destRemaining;
        } else {
            dest.put(src);
            markedPosition = src.position();
            src.position(srcStart);
            return markedPosition;
        }
    }

    @Override
    protected boolean encodeBody(EncodeContext context, ByteBuffer buf) {
        markedPosition = copy(body, buf, markedPosition);
        return markedPosition == body.limit();
    }
}
