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
package com.github.dtprj.dongting.buf;

import com.github.dtprj.dongting.common.DtException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * @author huangli
 */
public class RefBufferTest {

    private static final DefaultPoolFactoryConfig CFG = new DefaultPoolFactoryConfig();

    private SimpleByteBufferPoolConfig createDefaultConfig() {
        int[] sizes = CFG.smallSize;
        int[] minCount = new int[sizes.length];
        int[] maxCount = new int[sizes.length];
        Arrays.fill(minCount, 1);
        Arrays.fill(maxCount, 16);
        return new SimpleByteBufferPoolConfig(false, CFG.threshold,
                sizes, minCount, maxCount);
    }

    @Test
    public void testCreate1() {
        SimpleByteBufferPool pool = new SimpleByteBufferPool(createDefaultConfig());
        RefBuffer refBuffer = pool.newUnpooledRefBuffer(false, 127);
        refBuffer.retain();
        refBuffer.release();
        refBuffer.release();
        refBuffer.release();
        refBuffer.release();
    }

    @Test
    public void testCreate2() {
        SimpleByteBufferPool pool = new SimpleByteBufferPool(createDefaultConfig());
        RefBuffer refBuffer = pool.borrow(false, 128);
        refBuffer.retain();
        refBuffer.release();
        refBuffer.release();
        Assertions.assertThrows(DtException.class, refBuffer::release);
    }

    @Test
    public void testCreatePlain1() {
        SimpleByteBufferPool pool = new SimpleByteBufferPool(createDefaultConfig());
        RefBuffer refBuffer = pool.newUnpooledRefBuffer(true, 127);
        refBuffer.retain();
        refBuffer.release();
        refBuffer.release();
        refBuffer.release();
        refBuffer.release();
    }

    @Test
    public void testCreatePlain2() {
        SimpleByteBufferPool pool = new SimpleByteBufferPool(createDefaultConfig());
        RefBuffer refBuffer = pool.borrow(true, 128);
        refBuffer.retain();
        refBuffer.release();
        refBuffer.release();
        Assertions.assertThrows(DtException.class, refBuffer::release);
    }
}
