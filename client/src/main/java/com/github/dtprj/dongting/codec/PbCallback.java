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

import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.common.DtThread;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public abstract class PbCallback<T> {

    protected PbParser parser;

    public boolean readVarNumber(int index, long value) {
        return true;
    }

    public boolean readFix32(int index, int value) {
        return true;
    }

    public boolean readFix64(int index, long value) {
        return true;
    }

    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        return true;
    }

    void beforeParse(int len, PbParser parser) {
        this.parser = parser;
        begin(len);
    }

    void afterParse(boolean success) {
        this.parser = null;
        end(success);
    }

    protected void begin(int len) {
    }

    protected void end(boolean success) {
    }

    public T getResult() {
        throw new UnsupportedOperationException();
    }

    protected final String parseUTF8(ByteBuffer buf, int fieldLen, int currentPos) {
        if (fieldLen == 0) {
            return "";
        }
        int needRead = fieldLen - currentPos;
        int remain = buf.remaining();
        byte[] result;
        if (remain < needRead) {
            if (currentPos == 0) {
                result = new byte[fieldLen];
                parser.attachment = result;
            } else {
                result = (byte[]) parser.attachment;
            }
            buf.get(result, currentPos, remain);
            return null;
        } else {
            DtThread thread = (DtThread) Thread.currentThread();
            if (currentPos == 0) {
                byte[] cache = thread.getCache();
                if (cache.length < fieldLen) {
                    result = new byte[fieldLen];
                } else {
                    result = cache;
                }
            } else {
                result = (byte[]) parser.attachment;
            }
            buf.get(result, currentPos, needRead);
            return new String(result, 0, fieldLen, StandardCharsets.UTF_8);
        }
    }

    protected final byte[] parseBytes(ByteBuffer buf, int fieldLen, int currentPos) {
        if (fieldLen == 0) {
            return new byte[0];
        }
        byte[] result;
        if (currentPos == 0) {
            result = new byte[fieldLen];
        } else {
            result = (byte[]) parser.attachment;
        }
        int needRead = fieldLen - currentPos;
        int remain = buf.remaining();
        if (remain < needRead) {
            buf.get(result, currentPos, remain);
            parser.attachment = result;
            return null;
        } else {
            buf.get(result, currentPos, needRead);
            return result;
        }
    }

    protected final ByteBuffer parseByteBuffer(ByteBuffer buf, int fieldLen, int currentPos) {
        if (fieldLen == 0) {
            return SimpleByteBufferPool.EMPTY_BUFFER;
        }
        ByteBuffer result;
        if (currentPos == 0) {
            result = ByteBuffer.allocate(fieldLen);
        } else {
            result = (ByteBuffer) parser.attachment;
        }
        result.put(buf);
        if (result.position() < fieldLen) {
            parser.attachment = result;
            return null;
        } else {
            result.flip();
            return result;
        }
    }
}
