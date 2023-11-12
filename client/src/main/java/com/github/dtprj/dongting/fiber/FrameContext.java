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
public class FrameContext<O> {

    final Dispatcher dispatcher;

    @SuppressWarnings("rawtypes")
    FiberFrame frame;
    Fiber fiber;
    FiberGroup group;

    FrameContext(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public <I, O> FiberFrame<I, O> newFrame(FrameCall<I, O> body,
                                            FrameCall<Throwable, O> catchClause,
                                            FrameCall<Void, O> finallyClause) {
        return new FiberFrame<>(body, catchClause, finallyClause);
    }

    public <I, O> FiberFrame<I, O> newFrame(FrameCall<I, O> body,
                                            FrameCall<Throwable, O> catchClause) {
        return new FiberFrame<>(body, catchClause);
    }

    public <I, O> FiberFrame<I, O> newFrame(FrameCall<I, O> body) {
        return new FiberFrame<>(body);
    }

    public <I2, O2> FrameCallResult call(I2 input, FiberFrame<I2, O2> subFrame, FrameCall<O2, O> resumePoint) {
        dispatcher.call(fiber, input, frame, subFrame, resumePoint);
        return FrameCallResult.CALL_NEXT_FRAME;
    }

    public FrameCallResult awaitOn(FiberCondition c, FrameCall<Void, O> resumePoint) {
        dispatcher.awaitOn(frame, c, 0, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public FrameCallResult awaitOn(FiberCondition c, long millis, FrameCall<Void, O> resumePoint) {
        dispatcher.awaitOn(frame, c, millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public <T> FrameCallResult awaitOn(FiberFuture<T> f, FrameCall<T, O> resumePoint) {
        dispatcher.awaitOn(frame, f, 0, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public <T> FrameCallResult awaitOn(FiberFuture<T> f, long millis, FrameCall<T, O> resumePoint) {
        dispatcher.awaitOn(frame, f, millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }


    public FrameCallResult sleep(long millis, FrameCall<Void, O> resumePoint) {
        dispatcher.sleep(frame, millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public FrameCallResult frameReturn() {
        return FrameCallResult.RETURN;
    }

    public void setResult(O result) {
        frame.result = result;
    }

    public <T> FiberChannel<T> createOrGetChannel(int type) {
        return group.createOrGetChannel(type);
    }

    public Fiber getFiber() {
        return fiber;
    }

    public FiberGroup getFiberGroup() {
        return group;
    }

}
