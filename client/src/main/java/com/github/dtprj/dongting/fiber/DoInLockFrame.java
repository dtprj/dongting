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
public class DoInLockFrame<O> extends FiberFrame<O> {
    private final FiberLock lock;
    private final long timeoutMillis;
    private final FiberFrame<O> subFrame;
    private boolean locked;

    public DoInLockFrame(FiberLock lock, long timeoutMillis, FiberFrame<O> subFrame) {
        this.lock = lock;
        this.timeoutMillis = timeoutMillis;
        this.subFrame = subFrame;
    }

    @Override
    public FrameCallResult execute(Void input) throws Exception {
        return lock.lock(this, timeoutMillis, this::afterGetLock);
    }

    private FrameCallResult afterGetLock(Void v) {
        locked = true;
        return call(subFrame, this::afterSubFrameReturn);
    }

    private FrameCallResult afterSubFrameReturn(O o) {
        setResult(o);
        return frameReturn();
    }

    @Override
    protected FrameCallResult doFinally() {
        if (locked) {
            lock.unlock();
        }
        locked = false;
        return frameReturn();
    }
}
