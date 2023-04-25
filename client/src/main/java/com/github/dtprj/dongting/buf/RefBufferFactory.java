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

/**
 * @author huangli
 */
public class RefBufferFactory {
    private final ByteBufferPool pool;
    private final int threshold;

    public RefBufferFactory(ByteBufferPool pool, int threshold) {
        this.pool = pool;
        this.threshold = threshold;
    }

    public RefBuffer create(int requestSize) {
        return new RefBuffer(false, pool, requestSize, threshold);
    }

    public RefBuffer createPlain(int requestSize) {
        return new RefBuffer(true, pool, requestSize, threshold);
    }
}
