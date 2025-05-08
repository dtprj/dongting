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
package com.github.dtprj.dongting.common;

import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author huangli
 */
public class ByteArray implements Encodable {
    private final byte[] data;
    private byte[] copy;
    private final int startPos;
    public final int length;
    private int hash;

    private String str;

    public static ByteArray EMPTY = new ByteArray(new byte[0], 0, 0);

    public ByteArray(byte[] data) {
        this(data, 0, data.length);
    }

    public ByteArray(byte[] data, int startPos, int length) {
        Objects.requireNonNull(data);
        int l = data.length;
        if (startPos < 0 || startPos > l) {
            throw new IllegalArgumentException("startPos: " + startPos + ", array length: " + data.length);
        }
        if (length < 0 || startPos + length > l) {
            throw new IllegalArgumentException("len: " + length + ", startPos: " + startPos + ", array length: " + data.length);
        }
        this.data = data;
        this.startPos = startPos;
        this.length = length;
    }

    public int lastIndexOf(byte b) {
        for (int i = length - 1; i >= 0; i--) {
            if (data[startPos + i] == b) {
                return i;
            }
        }
        return -1;
    }

    public ByteArray sub(int begin) {
        return new ByteArray(data, startPos + begin, length - begin);
    }

    public ByteArray sub(int begin, int end) {
        return new ByteArray(data, startPos + begin, end - begin);
    }

    public byte get(int i) {
        if (i < 0 || i >= length) {
            throw new IndexOutOfBoundsException(String.valueOf(i));
        }
        return data[startPos + i];
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ByteArray) {
            ByteArray x = (ByteArray) obj;
            if (length == x.length) {
                for (int i = length - 1; i >= 0; i--) {
                    if (data[startPos + i] != x.data[x.startPos + i]) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int h = 1;
            for (int i = 0; i < length; i++) {
                h = 31 * h + data[startPos + i];
            }
            if (h == 0) {
                h = 1;
            }
            this.hash = h;
        }
        return hash;
    }

    @Override
    public String toString() {
        if (str == null) {
            str = new String(data, startPos, length);
        }
        return str;
    }

    public byte[] getData() {
        if (startPos == 0 && length == data.length) {
            return data;
        } else {
            if (copy != null) {
                return copy;
            } else {
                byte[] ret = new byte[length];
                System.arraycopy(data, startPos, ret, 0, length);
                this.copy = ret;
                return ret;
            }
        }
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer buffer) {
        if (length == 0) {
            return true;
        }
        int remaining = buffer.remaining();
        int needWrite = length - context.pending;
        if (remaining >= needWrite) {
            buffer.put(data, startPos + context.pending, needWrite);
            context.pending = 0;
            return true;
        } else {
            buffer.put(data, startPos + context.pending, remaining);
            context.pending += remaining;
            return false;
        }
    }

    public boolean isSlice() {
        return startPos > 0 || length < data.length;
    }

    @Override
    public int actualSize() {
        return length;
    }

    public static final class Callback extends DecoderCallback<ByteArray> {

        private byte[] r;

        @Override
        public boolean doDecode(ByteBuffer buffer, int bodyLen, int currentPos) {
            r = parseBytes(buffer, bodyLen, currentPos);
            return true;
        }

        @Override
        protected ByteArray getResult() {
            if (r == null) {
                return EMPTY;
            } else {
                return new ByteArray(r);
            }
        }

        @Override
        protected boolean end(boolean success) {
            this.r = null;
            return success;
        }
    }
}
