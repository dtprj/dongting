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

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.codec.PbParser;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
class MultiParser {
    private final int maxSize;
    private final PbCallback<?> callback;
    private final PbParser parser;

    private int size;
    private boolean parseLen = true;
    private int pendingLenBytes;

    public MultiParser(PbCallback<?> callback, int maxSize) {
        this.maxSize = maxSize;
        this.callback = callback;
        this.parser = new PbParser();
    }

    public void parse(ByteBuffer buf) {
        while (buf.hasRemaining()) {
            if (parseLen) {
                parseLen(buf);
                if (parseLen) {
                    return;
                } else {
                    parser.prepareNext(callback, size);
                }
            }
            parser.parse(buf);
            if (parser.isFinished()) {
                parseLen = true;
            }
        }
    }

    private void parseLen(ByteBuffer buf) {
        // read buffer is little endian.
        // however the length field is out of proto buffer data, and it's big endian
        int remain = buf.remaining();
        if (pendingLenBytes == 0 && remain >= 4) {
            int len = buf.getInt();
            if (len < 0 || len > maxSize) {
                throw new PbException("maxSize exceed: max=" + maxSize + ", actual=" + len);
            }
            this.size = len;
            parseLen = false;
            return;
        }
        int restLen = 4 - pendingLenBytes;
        if (remain >= restLen) {
            for (int i = 0; i < restLen; i++) {
                size = (size << 8) | (0xFF & buf.get());
            }
            if (size < 0 || size > maxSize) {
                throw new PbException("maxSize exceed: max=" + maxSize + ", actual=" + size);
            }
            pendingLenBytes = 0;
            parseLen = false;
        } else {
            for (int i = 0; i < remain; i++) {
                size = (size << 8) | (0xFF & buf.get());
            }
            pendingLenBytes += remain;
        }
    }


    public void reset() {
        parser.reset();
    }
}
