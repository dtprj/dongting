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
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    private FiberQueueTask head;
    private FiberQueueTask tail;
    private boolean shutdown;

    public FiberQueue() {
    }

    public boolean offer(FiberQueueTask task) {
        lock.lock();
        try {
            if (shutdown) {
                return false;
            }
            if (head == null) {
                head = tail = task;
                notEmpty.signal();
            } else {
                tail.next = task;
                tail = task;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public FiberQueueTask poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        lock.lock();
        try {
            if (head == null) {
                notEmpty.await(timeout, timeUnit);
            }
            FiberQueueTask result = head;
            if (result.next == null) {
                head = tail = null;
            } else {
                head = result.next;
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    public void drainTo(ArrayList<FiberQueueTask> list) {
        lock.lock();
        try {
            FiberQueueTask task = head;
            while (task != null) {
                list.add(task);
                task = task.next;
            }
            head = tail = null;
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

