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
package com.github.dtprj.dongting.fiber;

import com.github.dtprj.dongting.common.IndexedQueue;

/**
 * @author huangli
 */
public class FiberChannel<T> {
    private final IndexedQueue<T> queue;
    private final FiberCondition notEmptyCondition;

    FiberChannel(FiberGroup group) {
        this(group, 8);
    }

    FiberChannel(FiberGroup group, int initSize) {
        this.queue = new IndexedQueue<>(initSize);
        this.notEmptyCondition = group.newCondition();
    }

    public boolean offer(T data) {
        queue.addLast(data);
        if (queue.size() == 1) {
            notEmptyCondition.signal();
        }
        return true;
    }

    public FrameCallResult take(FrameCall<T> resumePoint) throws Throwable {
        if (queue.size() > 0) {
            T data = queue.removeFirst();
            return resumePoint.execute(data);
        } else {
            return notEmptyCondition.await(noUseVoid -> {
                T data = queue.removeFirst();
                return resumePoint.execute(data);
            });
        }
    }
}
