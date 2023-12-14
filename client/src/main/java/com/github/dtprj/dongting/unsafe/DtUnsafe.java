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
package com.github.dtprj.dongting.unsafe;

import com.github.dtprj.dongting.common.DtException;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * This class is compiled as java source 8, target 8, but without release flag.
 * The jdk.internal.misc.Unsafe need set --add-opens at runtime, so we not use it.
 * Use sun.misc.Unsafe can't set --release flag of javac.
 * Notice not all method of this class is available in java 8.
 *
 * @author huangli
 */
public class DtUnsafe {
    private static final Unsafe UNSAFE;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new DtException(e);
        }
    }

    /**
     * This method can be used in java 8/11/17/21.
     */
    public static void releaseFence() {
        UNSAFE.storeFence();
    }

    /**
     * This method can be used in java 8/11/17/21.
     */
    public static void acquireFence() {
        UNSAFE.loadFence();
    }

    /**
     * This method can be used in java 8/11/17/21.
     */
    public static void fullFence() {
        UNSAFE.fullFence();
    }

    /**
     * This method can't be used in java 8. It can be used in java 11/17/21.
     */
    public static void freeDirectBuffer(ByteBuffer buffer) {
        UNSAFE.invokeCleaner(buffer);
    }
}
