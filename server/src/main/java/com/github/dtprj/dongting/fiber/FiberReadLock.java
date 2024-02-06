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
public class FiberReadLock extends Lock {

    int heldCount;
    private final FiberLock writeLock;

    public FiberReadLock(FiberGroup group, FiberLock writeLock) {
        super(group);
        this.writeLock = writeLock;
    }

    private boolean shouldWait(Fiber currentFiber) {
        return writeLock.owner != null && writeLock.owner != currentFiber;
    }

    @Override
    protected void prepare(Fiber fiber, boolean timeout) {
        if (fiber.scheduleTimeoutMillis > 0) {
            fiber.inputObj = timeout ? Boolean.FALSE : Boolean.TRUE;
        } else {
            fiber.inputObj = null;
        }
        heldCount++;
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
            heldCount++;
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
            heldCount++;
            return Fiber.resume(Boolean.TRUE, resumePoint);
        }
    }

    @Override
    public boolean tryLock() {
        Fiber fiber = Dispatcher.getCurrentFiberAndCheck(fiberGroup);
        if (shouldWait(fiber)) {
            return false;
        } else {
            heldCount++;
            return true;
        }
    }

    @Override
    public void unlock() {
        Dispatcher.getCurrentFiberAndCheck(fiberGroup);
        // check fiber held this read lock?
        heldCount--;
        if (heldCount <= 0) {
            if (writeLock.firstWaiter != null) {
                writeLock.signal0(true);
            }
        }
    }
}
