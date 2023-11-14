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
public class FiberFrame<I, O> {
    Fiber fiber;
    FiberFrame prev;

    FrameCall<I, O> body;
    FrameCall<Throwable, O> catchClause;
    FrameCall<Void, O> finallyClause;
    FrameCall resumePoint;

    O result;

    FiberFrame() {
    }

    public static <I, O> FiberFrame<I, O> create(Fiber fiber, FrameCall<I, O> body,
                                                 FrameCall<Throwable, O> catchClause,
                                                 FrameCall<Void, O> finallyClause) {
        FiberFrame<I, O> frame = new FiberFrame<>();
        frame.fiber = fiber;
        frame.body = body;
        frame.catchClause = catchClause;
        frame.finallyClause = finallyClause;
        return frame;
    }

    public static <I, O> FiberFrame<I, O> create(Fiber fiber, FrameCall<I, O> body) {
        return create(fiber, body, null, null);
    }

    public <I2, O2> FrameCallResult suspendCall(I2 input, FiberFrame<I2, O2> subFrame, FrameCall<O2, O> resumePoint) {
        fiber.fiberGroup.dispatcher.suspendCall(input, this, subFrame, resumePoint);
        return FrameCallResult.CALL_NEXT_FRAME;
    }

    public FrameCallResult awaitOn(FiberCondition c, FrameCall<Void, O> resumePoint) {
        fiber.fiberGroup.dispatcher.awaitOn(this, c, 0, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public FrameCallResult awaitOn(FiberCondition c, long millis, FrameCall<Void, O> resumePoint) {
        fiber.fiberGroup.dispatcher.awaitOn(this, c, millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public <T> FrameCallResult awaitOn(FiberFuture<T> f, FrameCall<T, O> resumePoint) {
        fiber.fiberGroup.dispatcher.awaitOn(this, f, 0, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public <T> FrameCallResult awaitOn(FiberFuture<T> f, long millis, FrameCall<T, O> resumePoint) {
        fiber.fiberGroup.dispatcher.awaitOn(this, f, millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }


    public FrameCallResult sleep(long millis, FrameCall<Void, O> resumePoint) {
        fiber.fiberGroup.dispatcher.sleep(this, millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public FrameCallResult frameReturn() {
        return FrameCallResult.RETURN;
    }

    public void setResult(O result) {
        this.result = result;
    }

    public <T> FiberChannel<T> createOrGetChannel(int type) {
        return fiber.fiberGroup.createOrGetChannel(type);
    }

    public Fiber getFiber() {
        return fiber;
    }

    public FiberGroup getFiberGroup() {
        return fiber.fiberGroup;
    }

    public <O2> FiberFrame<I, O2> then(FrameCall<O, O2> nextAction) {
         FrameCall<I, O2> body = (currentFrame, input) ->
                 currentFrame.suspendCall(input, FiberFrame.this, nextAction);
        return create(fiber, body);
    }
}
