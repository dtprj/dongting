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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author huangli
 */
public class BitUtilTest {
    @Test
    public void testNextHighestPowerOfTwo() {
        assertThrows(IllegalArgumentException.class, () -> BitUtil.nextHighestPowerOfTwo(0));
        assertThrows(IllegalArgumentException.class, () -> BitUtil.nextHighestPowerOfTwo(-1));
        assertEquals(1, BitUtil.nextHighestPowerOfTwo(1));
        assertEquals(2, BitUtil.nextHighestPowerOfTwo(2));
        assertEquals(4, BitUtil.nextHighestPowerOfTwo(3));
        assertEquals(4, BitUtil.nextHighestPowerOfTwo(4));
        assertEquals(64, BitUtil.nextHighestPowerOfTwo(52));
        assertEquals(1 << 30, BitUtil.nextHighestPowerOfTwo((1 << 30) - 1));
        assertEquals(1 << 30, BitUtil.nextHighestPowerOfTwo(1 << 30));
        assertThrows(IllegalArgumentException.class, () -> BitUtil.nextHighestPowerOfTwo((1 << 30) + 1));
    }

    @Test
    public void testZeroCountOfBinary() {
        assertThrows(IllegalArgumentException.class, () -> BitUtil.zeroCountOfBinary(0));
        assertThrows(IllegalArgumentException.class, () -> BitUtil.zeroCountOfBinary(-1));
        assertThrows(IllegalArgumentException.class, () -> BitUtil.zeroCountOfBinary(3));
        assertThrows(IllegalArgumentException.class, () -> BitUtil.zeroCountOfBinary(1024 * 1024 * 1024 + 1));
        assertEquals(0, BitUtil.zeroCountOfBinary(1));
        assertEquals(1, BitUtil.zeroCountOfBinary(2));
        assertEquals(2, BitUtil.zeroCountOfBinary(4));
        assertEquals(3, BitUtil.zeroCountOfBinary(8));
        assertEquals(30, BitUtil.zeroCountOfBinary(1024 * 1024 * 1024));
    }

    @Test
    public void testNextHighestPowerOfTwo2() {
        assertThrows(IllegalArgumentException.class, () -> BitUtil.nextHighestPowerOfTwo(0L));
        assertThrows(IllegalArgumentException.class, () -> BitUtil.nextHighestPowerOfTwo(-1L));
        assertEquals(1, BitUtil.nextHighestPowerOfTwo(1L));
        assertEquals(2, BitUtil.nextHighestPowerOfTwo(2L));
        assertEquals(4, BitUtil.nextHighestPowerOfTwo(3L));
        assertEquals(4, BitUtil.nextHighestPowerOfTwo(4L));
        assertEquals(64, BitUtil.nextHighestPowerOfTwo(52L));
        assertEquals(1 << 62, BitUtil.nextHighestPowerOfTwo((1L << 62) - 1));
        assertEquals(1 << 62, BitUtil.nextHighestPowerOfTwo(1L << 62));
        assertThrows(IllegalArgumentException.class, () -> BitUtil.nextHighestPowerOfTwo((1L << 62) + 1));
    }

    @Test
    public void testZeroCountOfBinary2() {
        assertThrows(IllegalArgumentException.class, () -> BitUtil.zeroCountOfBinary(0L));
        assertThrows(IllegalArgumentException.class, () -> BitUtil.zeroCountOfBinary(-1L));
        assertThrows(IllegalArgumentException.class, () -> BitUtil.zeroCountOfBinary(3L));
        assertThrows(IllegalArgumentException.class, () -> BitUtil.zeroCountOfBinary((1 << 62) + 1));
        assertEquals(0, BitUtil.zeroCountOfBinary(1));
        assertEquals(1, BitUtil.zeroCountOfBinary(2));
        assertEquals(2, BitUtil.zeroCountOfBinary(4));
        assertEquals(3, BitUtil.zeroCountOfBinary(8));
        assertEquals(62, BitUtil.zeroCountOfBinary(1 << 62));
        assertThrows(IllegalArgumentException.class, () -> BitUtil.zeroCountOfBinary((1 << 62) + 1));
    }
}
