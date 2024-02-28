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

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
class FiberQueue {
    private static final FiberQueueTask TAIL = new FiberQueueTask() {
        @Override
        protected void run() {
        }
    };

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    private FiberQueueTask head = TAIL;
    private FiberQueueTask tail = TAIL;
    private boolean shutdown;

    public FiberQueue() {
    }

    public boolean offer(FiberQueueTask task) {
        lock.lock();
        try {
            if (shutdown) {
                return false;
            }
            if (task.next != null) {
                throw new FiberException("FiberQueueTask is already in queue");
            }
            if (head == TAIL) {
                head = tail = task;
                task.next = TAIL;
                notEmpty.signal();
            } else {
                tail.next = task;
                tail = task;
                task.next = TAIL;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public FiberQueueTask poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        lock.lock();
        try {
            if (head == TAIL) {
                if (!notEmpty.await(timeout, timeUnit)) {
                    return null;
                }
            }
            FiberQueueTask result = head;
            if (result.next == TAIL) {
                head = tail = TAIL;
            } else {
                head = result.next;
            }
            result.next = null;
            return result;
        } finally {
            lock.unlock();
        }
    }

    public void drainTo(ArrayList<FiberQueueTask> list) {
        lock.lock();
        try {
            FiberQueueTask task = head;
            while (task != TAIL) {
                list.add(task);
                FiberQueueTask tmp = task;
                task = task.next;
                tmp.next = null;
            }
            head = tail = TAIL;
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() {
        lock.lock();
        try {
            shutdown = true;
        } finally {
            lock.unlock();
        }
    }
}

