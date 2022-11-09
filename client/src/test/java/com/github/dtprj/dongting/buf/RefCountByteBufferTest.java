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

/**
 * @author huangli
 */
public class RefCountByteBufferTest {

    @Test
    public void testCreate1() {
        ByteBufferPool pool = new ByteBufferPool(false);
        RefCountByteBuffer refCountByteBuffer = RefCountByteBuffer.create(pool, 127, 128);
        refCountByteBuffer.retain();
        refCountByteBuffer.release();
        refCountByteBuffer.release();
        refCountByteBuffer.release();
        refCountByteBuffer.release();
    }

    @Test
    public void testCreate2() {
        ByteBufferPool pool = new ByteBufferPool(false);
        RefCountByteBuffer refCountByteBuffer = RefCountByteBuffer.create(pool, 128, 128);
        refCountByteBuffer.retain();
        refCountByteBuffer.release();
        refCountByteBuffer.release();
        Assertions.assertThrows(DtException.class, refCountByteBuffer::release);
    }

    @Test
    public void testCreatePlain1() {
        ByteBufferPool pool = new ByteBufferPool(false);
        RefCountByteBuffer refCountByteBuffer = RefCountByteBuffer.createPlain(pool, 127, 128);
        refCountByteBuffer.retain();
        refCountByteBuffer.release();
        refCountByteBuffer.release();
        refCountByteBuffer.release();
        refCountByteBuffer.release();
    }

    @Test
    public void testCreatePlain2() {
        ByteBufferPool pool = new ByteBufferPool(false);
        RefCountByteBuffer refCountByteBuffer = RefCountByteBuffer.createPlain(pool, 128, 128);
        refCountByteBuffer.retain();
        refCountByteBuffer.release();
        refCountByteBuffer.release();
        Assertions.assertThrows(DtException.class, refCountByteBuffer::release);
    }
}
