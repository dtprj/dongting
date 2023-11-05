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
public abstract class FiberFrame {
    Fiber fiber;
    FiberGroup group;
    FiberFrame prev;
    boolean bodyFinished;
    boolean finallyCalled;
    boolean handleCalled;

    Runnable resumePoint;

    int outputInt;
    long outputLong;
    Object outputObj;

    protected abstract void execute();

    protected void doFinally() {
    }

    protected void suspendCall(FiberFrame fiberFrame, Runnable resumePoint) {
        if (this.resumePoint != null) {
            throw new FiberException("already suspended");
        }
        this.resumePoint = resumePoint;
        fiberFrame.group = this.group;
        fiberFrame.fiber = this.fiber;
        group.dispatcher.suspendCall(this, fiberFrame);
    }

    protected Fiber getFiber() {
        return fiber;
    }

    protected FiberGroup getGroup() {
        return group;
    }

    protected final void setOutputObj(Object obj) {
        outputObj = obj;
    }

    protected final void setOutputInt(int v) {
        outputInt = v;
    }

    protected final void setOutputLong(long v) {
        outputLong = v;
    }

    protected final Object getLastResultObj() {
        return group.dispatcher.lastResultObj;
    }

    protected final int getLastResultInt() {
        return group.dispatcher.lastResultInt;
    }

    protected final long getLastResultLong() {
        return group.dispatcher.lastResultLong;
    }
}
