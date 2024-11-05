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
package com.github.dtprj.dongting.dtkv.server;

import java.util.Objects;

/**
 * @author huangli
 */
final class ByteArray {
    private final byte[] data;
    private final int startPos;
    private final int len;
    private final int hash;

    private String str;

    public static ByteArray EMPTY = new ByteArray(new byte[0], 0, 0);

    public ByteArray(byte[] data) {
        this(data, 0, data.length);
    }

    public ByteArray(byte[] data, int startPos, int len) {
        Objects.requireNonNull(data);
        this.data = data;
        this.startPos = startPos;
        this.len = len;
        int h = 1;
        for (int i = 0; i < len; i++) {
            h = 31 * h + data[startPos + i];
        }
        this.hash = h;
    }

    public int lastIndexOf(byte b) {
        for (int i = len - 1; i >= 0; i--) {
            if (data[startPos + i] == b) {
                return i;
            }
        }
        return -1;
    }

    public ByteArray sub(int begin) {
        return new ByteArray(data, startPos + begin, len - begin);
    }

    public ByteArray sub(int begin, int end) {
        return new ByteArray(data, startPos + begin, end - begin);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ByteArray) {
            ByteArray x = (ByteArray) obj;
            if (len == x.len) {
                for (int i = len - 1; i >= 0; i--) {
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
        return hash;
    }

    @Override
    public String toString() {
        if (str == null) {
            str = new String(data, startPos, len);
        }
        return str;
    }

    public byte[] getData() {
        if (startPos == 0 && len == data.length) {
            return data;
        } else {
            byte[] ret = new byte[len];
            System.arraycopy(data, startPos, ret, 0, len);
            return ret;
        }
    }
}
