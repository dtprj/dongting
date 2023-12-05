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
public abstract class FiberFrame<O> implements FrameCall<Void> {
    static final int STATUS_INIT = 0;
    static final int STATUS_BODY_CALLED = 1;
    static final int STATUS_CATCH_CALLED = 2;
    static final int STATUS_FINALLY_CALLED = 3;
    Fiber fiber;
    FiberGroup fiberGroup;
    FiberFrame prev;
    int status;

    FrameCall resumePoint = this;

    O result;

    protected FrameCallResult doFinally() {
        return FrameCallResult.RETURN;
    }

    protected FrameCallResult handle(Throwable ex) throws Throwable {
        throw ex;
    }

    void reset(Fiber f) {
        if (this.status != STATUS_INIT && this.status != STATUS_FINALLY_CALLED) {
            throw new FiberException("frame is in use");
        }
        this.fiber = f;
        this.fiberGroup = f.fiberGroup;
        this.prev = null;
        this.status = STATUS_INIT;
        this.resumePoint = null;
        this.result = null;
    }

    protected boolean isGroupShouldStopPlain() {
        return fiberGroup.isShouldStopPlain();
    }

    protected void setResult(O result){
        this.result = result;
    }

    protected Fiber getFiber() {
        return fiber;
    }

    protected FiberGroup getFiberGroup() {
        return fiberGroup;
    }

    protected boolean finished(Fiber fiber) {
        return fiber.finished;
    }

    protected FrameCallResult justReturn(O result) {
        setResult(result);
        return Fiber.frameReturn();
    }

    public static <O> FiberFrame<O> completedFrame(O result) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                setResult(result);
                return Fiber.frameReturn();
            }
        };
    }


    private static final FiberFrame<Void> VOID_COMPLETED_FRAME = new FiberFrame<>() {
        @Override
        public FrameCallResult execute(Void input) {
            return Fiber.frameReturn();
        }
    };
    public static FiberFrame<Void> voidCompletedFrame() {
        return VOID_COMPLETED_FRAME;
    }
}
