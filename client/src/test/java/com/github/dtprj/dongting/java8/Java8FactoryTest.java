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
package com.github.dtprj.dongting.java8;

import com.github.dtprj.dongting.common.VersionFactory;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author huangli
 */
public class Java8FactoryTest {
    @Test
    public void testJava8Factory() {
        VersionFactory f = new Java8Factory();
        f.newRefCountUpdater(true);
        f.newRefCountUpdater(false);
        f.newMpscLinkedQueue();
        ByteBuffer buffer = ByteBuffer.allocateDirect(1);
        f.releaseDirectBuffer(buffer);
    }

    @Test
    public void testGetIntGetLong() {
        VersionFactory f = new Java8Factory();
        byte[] bytes = new byte[16];
        ByteBuffer.wrap(bytes).putInt(0, 0xF1F2F3F4).putLong(4, 0x0102030405060708L);
        assertEquals(0xF1F2F3F4, f.getInt(bytes, 0));
        assertEquals(0x01020304, f.getInt(bytes, 4));
        assertEquals(0x05060708, f.getInt(bytes, 8));
        assertEquals(0xF1F2F3F401020304L, f.getLong(bytes, 0));
        assertEquals(0x0102030405060708L, f.getLong(bytes, 4));

        ByteBuffer.wrap(bytes).putLong(3, 0x8899AABBCCDDEEFFL);
        assertEquals(0x8899AABBCCDDEEFFL, f.getLong(bytes, 3));

        assertThrows(ArrayIndexOutOfBoundsException.class, () -> f.getInt(new byte[3], 0));
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> f.getLong(new byte[8], -1));
    }
}
