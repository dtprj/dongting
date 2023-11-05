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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

/**
 * @author huangli
 */
abstract class WaitSource {
    private static final DtLog log = DtLogs.getLogger(FiberCondition.class);
    private Fiber waiter;
    private IndexedQueue<Fiber> waitQueue;
    protected final FiberGroup group;

    public WaitSource(FiberGroup group) {
        this.group = group;
    }

    public void await(FiberFrame resumeFrame) {
        Fiber f = group.getCurrentFiber();
        if (f == null) {
            throw new FiberException("current fiber is null");
        }
        f.awaitOn(this, resumeFrame);
    }

    void addWaiter(Fiber f) {
        if (waiter == null) {
            waiter = f;
        } else {
            if (waitQueue == null) {
                waitQueue = new IndexedQueue<>(8);
                waitQueue.addLast(waiter);
                waiter = f;
            }
        }
    }

    private Fiber nextWaiter() {
        Fiber f = waiter;
        if (f != null) {
            waiter = null;
            return f;
        }
        if (waitQueue != null) {
            return waitQueue.removeFirst();
        }
        return null;
    }

    void signal0() {
        if (group.finished()) {
            log.warn("group finished, ignore signal: {}", group.getName());
            return;
        }
        Fiber f = nextWaiter();
        if (f != null) {
            group.makeReady(f);
        }
    }

    void signalAll0() {
        if (group.finished()) {
            log.warn("group finished, ignore signalAll: {}", group.getName());
            return;
        }
        Fiber f;
        while ((f = nextWaiter()) != null) {
            group.makeReady(f);
        }
    }
}
