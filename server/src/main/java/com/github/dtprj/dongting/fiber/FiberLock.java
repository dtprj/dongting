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

import com.github.dtprj.dongting.common.DtUtil;

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

    @Override
    protected boolean throwWhenTimeout() {
        return false;
    }

    @Override
    protected void prepare(Fiber fiber, FiberFrame<?> fiberFrame) {
        Object inputObj;
        if (owner == null) {
            owner = fiber;
            count = 1;
        } else if (fiber == owner) {
            count++;
        }
        if (fiber.scheduleTimeoutMillis >= 0) {
            inputObj = owner == fiber ? Boolean.TRUE : Boolean.FALSE;
        } else {
            inputObj = null;
        }
        fiber.inputObj = inputObj;
    }

    public FrameCallResult lock(FrameCall<Void> resumePoint) {
        Fiber fiber = Dispatcher.getCurrentFiberAndCheck(fiberGroup);
        return Dispatcher.awaitOn(fiber, this, -1, resumePoint);
    }

    public FrameCallResult tryLock(long millis, FrameCall<Boolean> resumePoint) {
        DtUtil.checkPositive(millis, "millis");
        Fiber fiber = Dispatcher.getCurrentFiberAndCheck(fiberGroup);
        return Dispatcher.awaitOn(fiber, this, millis, resumePoint);
    }

    public boolean tryLock() {
        Fiber fiber = Dispatcher.getCurrentFiberAndCheck(fiberGroup);

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
        return owner == Dispatcher.getCurrentFiberAndCheck(fiberGroup);
    }

    public void unlock() {
        Fiber fiber = Dispatcher.getCurrentFiberAndCheck(fiberGroup);
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
