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
import com.github.dtprj.dongting.common.Timestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author huangli
 */
public class RefByteBufferTest {

    @Test
    public void testCreate1() {
        SimpleByteBufferPool pool = new SimpleByteBufferPool(new Timestamp(), false);
        RefByteBuffer refByteBuffer = RefByteBuffer.create(pool, 128, 128);
        refByteBuffer.retain();
        refByteBuffer.release();
        refByteBuffer.release();
        refByteBuffer.release();
        refByteBuffer.release();
    }

    @Test
    public void testCreate2() {
        SimpleByteBufferPool pool = new SimpleByteBufferPool(new Timestamp(), false);
        RefByteBuffer refByteBuffer = RefByteBuffer.create(pool, 128, 127);
        refByteBuffer.retain();
        refByteBuffer.release();
        refByteBuffer.release();
        Assertions.assertThrows(DtException.class, refByteBuffer::release);
    }

    @Test
    public void testCreatePlain1() {
        SimpleByteBufferPool pool = new SimpleByteBufferPool(new Timestamp(), false);
        RefByteBuffer refByteBuffer = RefByteBuffer.createPlain(pool, 128, 128);
        refByteBuffer.retain();
        refByteBuffer.release();
        refByteBuffer.release();
        refByteBuffer.release();
        refByteBuffer.release();
    }

    @Test
    public void testCreatePlain2() {
        SimpleByteBufferPool pool = new SimpleByteBufferPool(new Timestamp(), false);
        RefByteBuffer refByteBuffer = RefByteBuffer.createPlain(pool, 128, 127);
        refByteBuffer.retain();
        refByteBuffer.release();
        refByteBuffer.release();
        Assertions.assertThrows(DtException.class, refByteBuffer::release);
    }
}
