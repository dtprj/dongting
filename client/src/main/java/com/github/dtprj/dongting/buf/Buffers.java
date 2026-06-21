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
        return threadSafeReleaseHeapPool.borrow(false, requestSize, 0);
    }

    public RefBuffer borrow(int requestSize, boolean plain) {
        return threadSafeReleaseHeapPool.borrow(plain, requestSize, 0);
    }

    public RefBuffer borrow(int requestSize, boolean plain, boolean threadSafeRelease, int threshold) {
        ByteBufferPool pool = threadSafeRelease ? threadSafeReleaseHeapPool : heapPool;
        return pool.borrow(plain, requestSize, threshold);
    }

    /**
     * Borrow a heap buffer for single-thread (local) usage: both borrow and release happen in the same thread.
     * Equivalent to {@code borrow(requestSize, true, false, 0)}.
     */
    public RefBuffer borrowLocal(int requestSize) {
        return heapPool.borrow(true, requestSize, 0);
    }

    public RefBuffer borrowDirect(int requestSize) {
        return threadSafeReleaseDirectPool.borrow(false, requestSize, 0);
    }

    public RefBuffer borrowDirect(int requestSize, boolean plain) {
        return threadSafeReleaseDirectPool.borrow(plain, requestSize, 0);
    }

    public RefBuffer borrowDirect(int requestSize, boolean plain, boolean threadSafeRelease, int threshold) {
        ByteBufferPool pool = threadSafeRelease ? threadSafeReleaseDirectPool : directPool;
        return pool.borrow(plain, requestSize, threshold);
    }

    /**
     * Borrow a direct buffer for single-thread (local) usage: both borrow and release happen in the same thread.
     * Equivalent to {@code borrowDirect(requestSize, true, false, 0)}.
     */
    public RefBuffer borrowDirectLocal(int requestSize) {
        return directPool.borrow(true, requestSize, 0);
    }

}
