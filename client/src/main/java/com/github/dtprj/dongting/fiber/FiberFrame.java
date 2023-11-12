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
@SuppressWarnings("rawtypes")
public abstract class FiberFrame<I, O> implements FrameCall<I> {
    Fiber fiber;
    FiberGroup fiberGroup;
    FiberFrame prev;
    boolean bodyFinished;
    boolean finallyCalled;
    boolean handleCalled;

    FrameCall resumePoint;

    O result;

    protected FrameCallResult doFinally() {
        return FrameCallResult.RETURN;
    }

    protected <I2, O2> FrameCallResult suspendCall(I2 input, FiberFrame<I2, O2> fiberFrame, FrameCall<O2> resumePoint) {
        fiberGroup.dispatcher.suspendCall(input, this, fiberFrame, resumePoint);
        return FrameCallResult.CALL_NEXT_FRAME;
    }

    protected FrameCallResult awaitOn(FiberCondition c, FrameCall<Void> resumePoint) {
        fiberGroup.dispatcher.awaitOn(this, c, 0, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    protected FrameCallResult awaitOn(FiberCondition c, long millis, FrameCall<Void> resumePoint) {
        fiberGroup.dispatcher.awaitOn(this, c, millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    protected <T> FrameCallResult awaitOn(FiberFuture<T> f, FrameCall<T> resumePoint) {
        fiberGroup.dispatcher.awaitOn(this, f, 0, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    protected <T> FrameCallResult awaitOn(FiberFuture<T> f, long millis, FrameCall<T> resumePoint) {
        fiberGroup.dispatcher.awaitOn(this, f, millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }


    protected FrameCallResult sleep(long millis, FrameCall<Void> resumePoint) {
        fiberGroup.dispatcher.sleep(this, millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    protected FrameCallResult frameReturn() {
        return FrameCallResult.RETURN;
    }

    protected void setResult(O result){
        this.result = result;
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

    protected <O2> FiberFrame<I, O2> then(FrameCall<O> nextAction) {
        FiberFrame<I, O2> nextFrame = new FiberFrame<I, O2>() {
            @Override
            public FrameCallResult execute(I input) {
                return suspendCall(input, FiberFrame.this, nextAction);
            }
        };
        return nextFrame;
    }
}
