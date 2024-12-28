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
    final IndexedQueue<T> queue;
    private final FiberCondition notEmptyCondition;
    private FiberCondition[] notEmptyAndShouldStop;

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
        return fireOffer(data, false);
    }

    public boolean fireOffer(T data, boolean failIfGroupShouldStop) {
        FiberQueueTask t = new FiberQueueTask(groupOfConsumer) {
            @Override
            protected void run() {
                offer0(data);
            }
        };
        t.failIfGroupShouldStop = failIfGroupShouldStop;
        return dispatcherOfConsumer.doInDispatcherThread(t);
    }

    public void offer(T data) {
        groupOfConsumer.checkGroup();
        offer0(data);
    }

    void offer0(T data) {
        queue.addLast(data);
        if (queue.size() == 1) {
            notEmptyCondition.signal0(true);
        }
    }

    /**
     * take from channel, may invoke resumePoint with null value.
     */
    public FrameCallResult take(FrameCall<T> resumePoint) {
        return take(-1, false, resumePoint);
    }

    /**
     * take from channel, may invoke resumePoint with null value.
     */
    public FrameCallResult take(boolean returnOnShouldStop, FrameCall<T> resumePoint) {
        return take(-1, returnOnShouldStop, resumePoint);
    }

    /**
     * take from channel, may invoke resumePoint with null value.
     *
     * @param millis timeout in milliseconds
     */
    public FrameCallResult take(long millis, FrameCall<T> resumePoint) {
        return take(millis, false, resumePoint);
    }

    /**
     * take from channel, may invoke resumePoint with null value.
     *
     * @param millis       timeout in milliseconds
     * @param returnOnShouldStop await should return immediately if group is stopping
     */
    public FrameCallResult take(long millis, boolean returnOnShouldStop, FrameCall<T> resumePoint) {
        groupOfConsumer.checkGroup();
        T data = queue.removeFirst();
        if (data != null || (returnOnShouldStop && groupOfConsumer.isShouldStopPlain())) {
            return Fiber.resume(data, resumePoint);
        } else {
            if (returnOnShouldStop) {
                if (notEmptyAndShouldStop == null) {
                    notEmptyAndShouldStop = new FiberCondition[]{notEmptyCondition, groupOfConsumer.getShouldStopCondition()};
                }
                return Dispatcher.awaitOn(notEmptyAndShouldStop, millis, noUseVoid -> afterTake(resumePoint));
            } else {
                return notEmptyCondition.await(millis, noUseVoid -> afterTake(resumePoint));
            }
        }
    }

    private FrameCallResult afterTake(FrameCall<T> resumePoint) {
        return Fiber.resume(queue.removeFirst(), resumePoint);
    }

    /**
     * take all elements from channel into given collection, may invoke resumePoint with empty collection.
     */
    public FrameCallResult takeAll(Collection<T> c, FrameCall<Void> resumePoint) {
        return takeAll(c, -1, false, resumePoint);
    }

    /**
     * take all elements from channel into given collection, may invoke resumePoint with empty collection.
     */
    public FrameCallResult takeAll(Collection<T> c, boolean returnOnShouldStop, FrameCall<Void> resumePoint) {
        return takeAll(c, -1, returnOnShouldStop, resumePoint);
    }

    /**
     * take all elements from channel into given collection, may invoke resumePoint with empty collection.
     *
     * @param millis timeout in milliseconds
     */
    public FrameCallResult takeAll(Collection<T> c, long millis, FrameCall<Void> resumePoint) {
        return takeAll(c, millis, false, resumePoint);
    }

    /**
     * take all elements from channel into given collection, may invoke resumePoint with empty collection.
     *
     * @param millis       timeout in milliseconds
     * @param returnOnShouldStop await should return immediately if group is stopping
     */
    public FrameCallResult takeAll(Collection<T> c, long millis, boolean returnOnShouldStop, FrameCall<Void> resumePoint) {
        groupOfConsumer.checkGroup();
        if (queue.size() > 0 || (returnOnShouldStop && groupOfConsumer.isShouldStopPlain())) {
            return afterTakeAll(c, resumePoint);
        } else {
            if (returnOnShouldStop) {
                if (notEmptyAndShouldStop == null) {
                    notEmptyAndShouldStop = new FiberCondition[]{notEmptyCondition, groupOfConsumer.getShouldStopCondition()};
                }
                return Dispatcher.awaitOn(notEmptyAndShouldStop, millis, noUseVoid -> afterTakeAll(c, resumePoint));
            } else {
                return notEmptyCondition.await(millis, noUseVoid -> afterTakeAll(c, resumePoint));
            }
        }
    }

    private FrameCallResult afterTakeAll(Collection<T> c, FrameCall<Void> resumePoint) {
        T data;
        while ((data = queue.removeFirst()) != null) {
            c.add(data);
        }
        return Fiber.resume(null, resumePoint);
    }
}
