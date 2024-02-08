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
public abstract class DoInLockFrame<O> extends FiberFrame<O> {
    private final Lock lock;
    private boolean locked;

    public DoInLockFrame(Lock lock) {
        this.lock = lock;
    }

    @Override
    public final FrameCallResult execute(Void input) throws Exception {
            return lock.lock(this::resume);
    }

    private FrameCallResult resume(Void unused) {
        this.locked = true;
        return afterGetLock();
    }

    protected abstract FrameCallResult afterGetLock();

    @Override
    protected final FrameCallResult doFinally() {
        if (locked) {
            lock.unlock();
        }
        locked = false;
        doFinallyAfterTryReleaseLock();
        return Fiber.frameReturn();
    }

    protected void doFinallyAfterTryReleaseLock() {
    }
}
