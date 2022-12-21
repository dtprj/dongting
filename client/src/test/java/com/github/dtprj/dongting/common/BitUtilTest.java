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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author huangli
 */
public class BitUtilTest {
    @Test
    public void testNextHighestPowerOfTwo() {
        Assertions.assertEquals(0, BitUtil.nextHighestPowerOfTwo(0));
        Assertions.assertEquals(1, BitUtil.nextHighestPowerOfTwo(1));
        Assertions.assertEquals(2, BitUtil.nextHighestPowerOfTwo(2));
        Assertions.assertEquals(4, BitUtil.nextHighestPowerOfTwo(3));
        Assertions.assertEquals(4, BitUtil.nextHighestPowerOfTwo(4));
        Assertions.assertEquals(64, BitUtil.nextHighestPowerOfTwo(52));
        Assertions.assertEquals(1 << 30, BitUtil.nextHighestPowerOfTwo((1 << 30) - 1));
    }
}
