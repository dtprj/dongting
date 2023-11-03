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
public abstract class Fiber {
    protected final FiberGroup fiberGroup;
    protected final String fiberName;
    boolean ready;
    boolean finished;

    WaitSource source;

    private FiberFrame stackBottom;
    private FiberFrame stackTop;

    public Fiber(FiberGroup fiberGroup, String fiberName, FiberFrame entryFrame) {
        this.fiberGroup = fiberGroup;
        this.fiberName = fiberName;
        this.stackBottom = entryFrame;
    }

    FiberFrame popFrame() {
        if (stackTop == null) {
            return null;
        } else {
            FiberFrame f = stackTop;
            stackTop = f.prev;
            if (stackTop == null) {
                stackBottom = null;
            }
            return f;
        }
    }

    public void insertCallback(FiberFrame frame) {
        frame.group = fiberGroup;
        frame.fiber = this;
        if (stackBottom == null) {
            stackBottom = frame;
            stackTop = frame;
        } else {
            stackBottom.prev = frame;
            stackBottom = frame;
        }
    }

    public void awaitOn(WaitSource c, FiberFrame resumeFrame) {
        if (!ready) {
            throw new IllegalStateException("fiber not ready state");
        }
        this.ready = false;
        this.source = c;
        insertCallback(resumeFrame);
        c.addWaiter(this);
    }

    public FiberCondition newCondition() {
        return fiberGroup.newCondition();
    }

    public FiberFuture newFuture() {
        return new FiberFuture(this.fiberGroup);
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
