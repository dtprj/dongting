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
public class FiberLock extends Lock {
    Fiber owner;
    int heldCount;
    private final FiberReadLock readLock;

    FiberLock(FiberGroup fiberGroup) {
        super(fiberGroup);
        this.readLock = new FiberReadLock(fiberGroup, this);
    }

    private boolean shouldWait(Fiber currentFiber) {
        return owner != null && currentFiber != owner || readLock.heldCount > 0;
    }

    @Override
    protected void prepare(Fiber fiber, boolean timeout) {
        if (fiber.scheduleTimeoutMillis > 0) {
            fiber.inputObj = timeout ? Boolean.FALSE : Boolean.TRUE;
        } else {
            fiber.inputObj = null;
        }
        updateOwnerAndHeldCount(fiber);
    }

    private void updateOwnerAndHeldCount(Fiber fiber) {
        if (owner == null) {
            owner = fiber;
            heldCount = 1;
        } else if (fiber == owner) {
            heldCount++;
        }
    }

    public FiberReadLock readLock() {
        return readLock;
    }

    @Override
    public FrameCallResult lock(FrameCall<Void> resumePoint) {
        return lock("waitLock", resumePoint);
    }

    @Override
    public FrameCallResult lock(String reason, FrameCall<Void> resumePoint) {
        Fiber fiber = Dispatcher.getCurrentFiberAndCheck(fiberGroup);
        if (shouldWait(fiber)) {
            return Dispatcher.awaitOn(fiber, this, -1, resumePoint, reason);
        } else {
            updateOwnerAndHeldCount(fiber);
            return Fiber.resume(null, resumePoint);
        }
    }

    @Override
    public FrameCallResult tryLock(long millis, FrameCall<Boolean> resumePoint) {
        return tryLock(millis, "timeWaitLock", resumePoint);
    }

    @Override
    public FrameCallResult tryLock(long millis, String reason, FrameCall<Boolean> resumePoint) {
        DtUtil.checkPositive(millis, "millis");
        Fiber fiber = Dispatcher.getCurrentFiberAndCheck(fiberGroup);
        if (shouldWait(fiber)) {
            return Dispatcher.awaitOn(fiber, this, millis, resumePoint, reason);
        } else {
            updateOwnerAndHeldCount(fiber);
            return Fiber.resume(Boolean.TRUE, resumePoint);
        }
    }

    @Override
    public boolean tryLock() {
        Fiber fiber = Dispatcher.getCurrentFiberAndCheck(fiberGroup);
        if (shouldWait(fiber)) {
            return false;
        } else {
            updateOwnerAndHeldCount(fiber);
            return true;
        }
    }

    public boolean isHeldByCurrentFiber() {
        return owner == Dispatcher.getCurrentFiberAndCheck(fiberGroup);
    }

    @Override
    public void unlock() {
        Fiber fiber = Dispatcher.getCurrentFiberAndCheck(fiberGroup);
        if (fiber == owner) {
            heldCount--;
            if (heldCount <= 0) {
                owner = null;
                if (firstWaiter != null) {
                    signal0(true);
                } else if (readLock.firstWaiter != null) {
                    readLock.signalAll0(true);
                }
            }
        } else {
            throw new FiberException("not owner");
        }
    }
}