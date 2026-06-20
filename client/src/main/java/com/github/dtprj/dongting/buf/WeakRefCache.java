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

import com.github.dtprj.dongting.common.IndexedQueue;

import java.lang.ref.WeakReference;

/**
 * @author huangli
 */
class WeakRefCache<T> {

    private final IndexedQueue<WeakReference<T>> queue;

    public WeakRefCache(int initialCapacity) {
        this.queue = new IndexedQueue<>(initialCapacity);
    }

    public T borrow() {
        IndexedQueue<WeakReference<T>> q = this.queue;
        while (q.size() > 0) {
            WeakReference<T> ref = q.pollLast();
            T v = ref.get();
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    public void releaseToCache(T value) {
        queue.addLast(new WeakReference<>(value));
    }

    public void moveIdleElementsToCache(T value) {
        // the buffer is not used recently, add to bottom
        queue.addFirst(new WeakReference<>(value));
    }

    public void cleanHeadAndTail() {
        IndexedQueue<WeakReference<T>> q = this.queue;
        WeakReference<T> ref = q.getFirst();
        if (ref == null) {
            return;
        }
        while (ref.get() == null) {
            q.pollFirst();
            ref = q.getFirst();
            if (ref == null) {
                return;
            }
        }
        ref = q.getLast();
        while (ref != null && ref.get() == null) {
            q.pollLast();
            ref = q.getLast();
        }
    }
}
