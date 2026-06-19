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
public class Buffers {

    final TwoLevelPool heapPool;
    final TwoLevelPool directPool;
    private ByteBufferPool threadSafeReleaseHeapPool;
    private ByteBufferPool threadSafeReleaseDirectPool;

    public Buffers(TwoLevelPool heapPool, TwoLevelPool directPool) {
        this.heapPool = heapPool;
        this.directPool = directPool;
    }

    public void init(ByteBufferPool threadSafeReleaseHeapPool, ByteBufferPool threadSafeReleaseDirectPool) {
        this.threadSafeReleaseHeapPool = threadSafeReleaseHeapPool;
        this.threadSafeReleaseDirectPool = threadSafeReleaseDirectPool;
    }

    public void clean() {
        heapPool.clean();
        directPool.clean();
    }

    public RefBuffer borrow(int requestSize) {
        return new RefBuffer(false, threadSafeReleaseHeapPool, requestSize, 0);
    }

    public RefBuffer borrow(int requestSize, boolean plain) {
        return new RefBuffer(plain, threadSafeReleaseHeapPool, requestSize, 0);
    }

    public RefBuffer borrow(int requestSize, boolean plain, boolean threadSafeRelease, int threshold) {
        return new RefBuffer(plain, threadSafeRelease ? threadSafeReleaseHeapPool : heapPool, requestSize, threshold);
    }

    public RefBuffer borrowDirect(int requestSize) {
        return new RefBuffer(false, threadSafeReleaseDirectPool, requestSize, 0);
    }

    public RefBuffer borrowDirect(int requestSize, boolean plain) {
        return new RefBuffer(plain, threadSafeReleaseDirectPool, requestSize, 0);
    }

    public RefBuffer borrowDirect(int requestSize, boolean plain, boolean threadSafeRelease, int threshold) {
        return new RefBuffer(plain, threadSafeRelease ? threadSafeReleaseDirectPool : directPool, requestSize, threshold);
    }

}
