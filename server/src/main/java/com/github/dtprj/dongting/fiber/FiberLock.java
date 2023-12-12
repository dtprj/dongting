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
public class FiberLock extends WaitSource {
    Fiber owner;
    int count;

    FiberLock(FiberGroup fiberGroup) {
        super(fiberGroup);
    }

    @Override
    protected boolean shouldWait(Fiber currentFiber) {
        return owner != null && currentFiber != owner;
    }

    private Fiber check() {
        Fiber fiber = Dispatcher.checkAndGetCurrentFiber();
        if (fiber.fiberGroup != group) {
            throw new FiberException("lock not belong to current fiber group");
        }
        return fiber;
    }

    public FrameCallResult lock(FrameCall<Void> resumePoint) {
        Fiber fiber = check();

        return Dispatcher.awaitOn(fiber, this, 0, v -> {
            if (owner == null) {
                owner = fiber;
                count = 1;
            } else if (fiber == owner) {
                count++;
            }
            return resumePoint.execute(null);
        });
    }

    public FrameCallResult tryLock(long millis, FrameCall<Boolean> resumePoint) {
        Fiber fiber = check();

        return Dispatcher.awaitOn(fiber, this, millis, v -> {
            if (owner == null) {
                owner = fiber;
                count = 1;
                return resumePoint.execute(Boolean.TRUE);
            } else if (fiber == owner) {
                count++;
                return resumePoint.execute(Boolean.TRUE);
            } else {
                return resumePoint.execute(Boolean.FALSE);
            }
        });
    }

    public boolean tryLock() {
        Fiber fiber = check();

        if (owner == null) {
            owner = fiber;
            count = 1;
            return true;
        } else if (fiber == owner) {
            count++;
            return true;
        } else {
            return false;
        }
    }

    public boolean isHeldByCurrentFiber() {
        return owner == check();
    }

    public void unlock() {
        Fiber fiber = check();
        if (fiber == owner) {
            count--;
            if (count <= 0) {
                owner = null;
                if (firstWaiter != null) {
                    // prevent other fiber takes the lock
                    owner = firstWaiter;
                    signal0();
                }
            }
        } else {
            throw new FiberException("not owner");
        }
    }
}
