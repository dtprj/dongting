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

import java.util.Collection;

/**
 * This queue is unbound and only block consumer.
 *
 * @author huangli
 */
public class FiberChannel<T> {
    private final FiberGroup groupOfConsumer;
    private final Dispatcher dispatcherOfConsumer;
    private final IndexedQueue<T> queue;
    private final FiberCondition notEmptyCondition;

    FiberChannel(FiberGroup groupOfConsumer) {
        this(groupOfConsumer, 64);
    }

    FiberChannel(FiberGroup groupOfConsumer, int initSize) {
        this.groupOfConsumer = groupOfConsumer;
        this.dispatcherOfConsumer = groupOfConsumer.dispatcher;
        this.queue = new IndexedQueue<>(initSize);
        this.notEmptyCondition = groupOfConsumer.newCondition("FiberChannelNotEmpty");
    }

    public boolean fireOffer(T data) {
        return dispatcherOfConsumer.doInDispatcherThread(new FiberQueueTask() {
            @Override
            protected void run() {
                offer0(data);
            }
        });
    }

    public void offer(T data) {
        groupOfConsumer.checkGroup();
        offer0(data);
    }

    private void offer0(T data) {
        queue.addLast(data);
        if (queue.size() == 1) {
            notEmptyCondition.signal0(true);
        }
    }

    public FrameCallResult take(FrameCall<T> resumePoint) {
        groupOfConsumer.checkGroup();
        T data = queue.removeFirst();
        if (data != null) {
            return Fiber.resume(data, resumePoint);
        } else {
            return notEmptyCondition.await(noUseVoid -> take(resumePoint));
        }
    }

    public FrameCallResult takeAll(Collection<T> c, FrameCall<Void> resumePoint) {
        groupOfConsumer.checkGroup();
        if (queue.size() > 0) {
            T data;
            while ((data = queue.removeFirst()) != null) {
                c.add(data);
            }
            return Fiber.resume(null, resumePoint);
        } else {
            return notEmptyCondition.await(noUseVoid -> takeAll(c, resumePoint));
        }
    }
}
