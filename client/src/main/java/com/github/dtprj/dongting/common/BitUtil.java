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

/**
 * @author huangli
 */
public class BitUtil {
    public static long toLong(int high, int low) {
        return ((long) high << 32) | (low & 0xFFFF_FFFFL);
    }

    public static int nextHighestPowerOfTwo(int v) {
        DtUtil.checkPositive(v, "value");
        if (v > 1 << 30) {
            throw new IllegalArgumentException(String.valueOf(v));
        }
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }

    public static int zeroCountOfBinary(int value) {
        DtUtil.checkPositive(value, "value");
        if (nextHighestPowerOfTwo(value) != value) {
            throw new IllegalArgumentException();
        }
        int count = 0;
        while (value != 1) {
            count++;
            value >>= 1;
        }
        return count;
    }

    public static long nextHighestPowerOfTwo(long v) {
        DtUtil.checkPositive(v, "value");
        if (v > 1L << 62) {
            throw new IllegalArgumentException(String.valueOf(v));
        }
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        v++;
        return v;
    }

    public static int zeroCountOfBinary(long value) {
        DtUtil.checkPositive(value, "value");
        if (nextHighestPowerOfTwo(value) != value) {
            throw new IllegalArgumentException();
        }
        int count = 0;
        while (value != 1) {
            count++;
            value >>= 1;
        }
        return count;
    }

}
