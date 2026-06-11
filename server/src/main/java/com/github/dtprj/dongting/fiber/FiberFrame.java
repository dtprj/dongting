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
    Fiber fiber;

    FiberFrame prev;
    boolean catchCalled;
    boolean finallyCalled;

    FrameCall resumePoint = this;

    O frameResult;
    Throwable frameEx;

    /**
     * Release resources held by this frame. This method exists because daemon
     * fibers are not guaranteed to finish when the fiber group shuts down:
     * {@code cleanup()} is called unconditionally on every daemon fiber frame
     * during group termination, while {@link #doFinally()} may be skipped.
     * On the normal exit path, {@code cleanup()} is called after
     * {@link #doFinally()} has fully completed (including any suspended
     * callbacks), and is guaranteed to run even if {@code doFinally()} throws.
     *
     * <p>Contract:
     * <ul>
     *   <li>Must NOT call any fiber API ({@code Fiber.sleep}, {@code awaitOn}, {@code yield},
     *       {@code Fiber.call}, etc.) — cleanup is synchronous and immediately followed by
     *       {@code popFrame()}, so suspension would corrupt the frame stack.</li>
     *   <li>Should only release resources (close files, release buffers, decrement
     *       counters).</li>
     *   <li>Should be null-safe and reentrant as a defensive measure.</li>
     *   <li>Exceptions are logged and swallowed; they do not affect the exit process.</li>
     * </ul>
     *
     * <p>The default implementation is empty. Override in subclasses that hold resources.
     */
    protected void cleanup() {
    }

    protected FrameCallResult doFinally() {
        return FrameCallResult.RETURN;
    }

    protected FrameCallResult handle(Throwable ex) throws Throwable {
        throw ex;
    }

    void init(Fiber f) {
        if (fiber == null) {
            this.fiber = f;
        } else {
            // this frame is reused
            if (fiber != f) {
                throw new FiberException("the frame not belongs to the fiber");
            }
            if (!finallyCalled) {
                throw new FiberException("the fiber frame is in use");
            }
            reset();
        }
    }

    private void reset() {
        prev = null;
        catchCalled = false;
        finallyCalled = false;
        resumePoint = this;
        frameResult = null;
        frameEx = null;
    }

    protected boolean isGroupShouldStopPlain() {
        return fiber.group.isShouldStopPlain();
    }

    protected void setResult(O result) {
        this.frameResult = result;
    }

    protected Fiber getFiber() {
        return fiber;
    }

    protected FiberGroup getFiberGroup() {
        return fiber.group;
    }

    protected FrameCallResult justReturn(O result) {
        setResult(result);
        return Fiber.frameReturn();
    }

    public static <O> FiberFrame<O> completedFrame(O theResult) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                setResult(theResult);
                return Fiber.frameReturn();
            }
        };
    }

    public static <O> FiberFrame<O> failedFrame(Exception theEx) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                throw theEx;
            }
        };
    }

    public static FiberFrame<Void> voidCompletedFrame() {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.frameReturn();
            }
        };
    }
}
