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

/**
 * @author huangli
 */
public abstract class Fiber {
    protected final FiberGroup fiberGroup;
    protected final String fiberName;
    boolean ready;
    boolean finished;

    WaitSource source;

    private FiberFrame stackBottom;
    private IndexedQueue<FiberFrame> stack;

    public Fiber(FiberGroup fiberGroup, String fiberName, FiberFrame entryFrame) {
        this.fiberGroup = fiberGroup;
        this.fiberName = fiberName;
        this.stackBottom = entryFrame;
    }

    FiberFrame popFrame() {
        IndexedQueue<FiberFrame> stack = this.stack;
        if (stack == null || stack.size() == 0) {
            FiberFrame f = stackBottom;
            this.stackBottom = null;
            return f;
        } else {
            return stack.removeLast();
        }
    }

    public void pushFrame(FiberFrame frame) {
        if (stackBottom == null) {
            stackBottom = frame;
            return;
        }
        if (stack == null) {
            stack = new IndexedQueue<>(8);
        }
        stack.addLast(frame);
    }

    public void awaitOn(WaitSource c, FiberFrame resumeFrame) {
        if (!ready) {
            throw new IllegalStateException("fiber not ready state");
        }
        this.ready = false;
        this.source = c;
        pushFrame(resumeFrame);
        c.addWaiter(this);
    }

    public FiberCondition newCondition() {
        return fiberGroup.newCondition();
    }

    public <T> FiberFuture<T> newFuture() {
        return new FiberFuture<>(this.fiberGroup);
    }

    public String getFiberName() {
        return fiberName;
    }

    boolean isReady() {
        return ready;
    }

    void setReady() {
        this.ready = true;
    }

    boolean isFinished() {
        return finished;
    }

    public FiberGroup getFiberGroup() {
        return fiberGroup;
    }
}
