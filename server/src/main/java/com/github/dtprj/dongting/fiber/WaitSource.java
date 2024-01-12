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

/**
 * @author huangli
 */
abstract class WaitSource {
    private Fiber lastWaiter;
    Fiber firstWaiter;
    protected final FiberGroup fiberGroup;

    public WaitSource(FiberGroup group) {
        this.fiberGroup = group;
    }

    protected abstract boolean shouldWait(Fiber currentFiber);

    protected abstract boolean throwWhenTimeout();

    protected abstract void prepare(Fiber fiber, FiberFrame<?> fiberFrame);


    void addWaiter(Fiber f) {
        if (firstWaiter == null) {
            firstWaiter = f;
        } else {
            lastWaiter.nextWaiter = f;
            f.previousWaiter = lastWaiter;
        }
        lastWaiter = f;
    }

    void removeWaiter(Fiber f) {
        if (f == firstWaiter && f == lastWaiter) {
            firstWaiter = null;
            lastWaiter = null;
        } else if (f == firstWaiter) {
            firstWaiter = f.nextWaiter;
            if (firstWaiter != null) {
                firstWaiter.previousWaiter = null;
            }
        } else if (f == lastWaiter) {
            lastWaiter = f.previousWaiter;
            if (lastWaiter != null) {
                lastWaiter.nextWaiter = null;
            }
        } else {
            f.previousWaiter.nextWaiter = f.nextWaiter;
            f.nextWaiter.previousWaiter = f.previousWaiter;
        }
        f.nextWaiter = null;
        f.previousWaiter = null;
    }

    Fiber popTailWaiter() {
        Fiber result = lastWaiter;
        if (result != null) {
            if (result == firstWaiter) {
                firstWaiter = null;
                lastWaiter = null;
            } else {
                lastWaiter = result.previousWaiter;
                lastWaiter.nextWaiter = null;
                result.previousWaiter = null;
            }
        }
        return result;
    }

    Fiber popHeadWaiter() {
        Fiber result = firstWaiter;
        if (result != null) {
            if (result == lastWaiter) {
                firstWaiter = null;
                lastWaiter = null;
            } else {
                firstWaiter = result.nextWaiter;
                firstWaiter.previousWaiter = null;
                result.nextWaiter = null;
            }
        }
        return result;
    }

    void signal0(boolean addFirst) {
        if (fiberGroup.finished) {
            return;
        }
        Fiber f = popHeadWaiter();
        if (f != null) {
            fiberGroup.dispatcher.tryRemoveFromScheduleQueue(f);
            fiberGroup.tryMakeFiberReady(f, addFirst);
            f.signalInThisRound = true;
        }
    }

    void signalAll0(boolean addFirst) {
        if (fiberGroup.finished) {
            return;
        }
        Fiber f;
        while ((f = popTailWaiter()) != null) {
            fiberGroup.dispatcher.tryRemoveFromScheduleQueue(f);
            fiberGroup.tryMakeFiberReady(f, addFirst);
            f.signalInThisRound = true;
        }
    }

    public FiberGroup getGroup() {
        return fiberGroup;
    }
}
