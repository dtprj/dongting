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
public abstract class FiberFrame implements FrameCall {
    Fiber fiber;
    FiberGroup fiberGroup;
    FiberFrame prev;
    boolean bodyFinished;
    boolean finallyCalled;
    boolean handleCalled;

    FrameCall resumePoint;

    int outputInt;
    long outputLong;
    Object outputObj;

    protected FrameCallResult doFinally() {
        return FrameCallResult.RETURN;
    }

    protected FrameCallResult suspendCall(FiberFrame fiberFrame, FrameCall resumePoint) {
        fiberGroup.dispatcher.suspendCall(this, fiberFrame, resumePoint);
        return FrameCallResult.CALL_NEXT_FRAME;
    }

    protected FrameCallResult awaitOn(WaitSource waitSource, FrameCall resumePoint) {
        fiberGroup.dispatcher.awaitOn(this, waitSource, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    protected FrameCallResult frameReturn() {
        return FrameCallResult.RETURN;
    }

    protected FrameCallResult sleep(long millis, FrameCall resumePoint) {
        fiberGroup.dispatcher.sleep(this, millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    protected <T> FiberChannel<T> createOrGetChannel(int type) {
        return fiberGroup.createOrGetChannel(type);
    }

    protected Fiber getFiber() {
        return fiber;
    }

    protected FiberGroup getFiberGroup() {
        return fiberGroup;
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
        return fiberGroup.dispatcher.lastResultObj;
    }

    protected final int getLastResultInt() {
        return fiberGroup.dispatcher.lastResultInt;
    }

    protected final long getLastResultLong() {
        return fiberGroup.dispatcher.lastResultLong;
    }
}
