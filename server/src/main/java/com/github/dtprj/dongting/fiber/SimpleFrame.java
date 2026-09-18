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
public final class SimpleFrame<O> extends FiberFrame<O> {

    private final FrameCall<SimpleFrame<O>> execute;
    private final FrameCall<Throwable> handle;
    private final FrameCall<Void> doFinally;

    public SimpleFrame(String methodName, FrameCall<SimpleFrame<O>> execute) {
        this(methodName, execute, null, null);
    }

    public SimpleFrame(String methodName, FrameCall<SimpleFrame<O>> execute, FrameCall<Throwable> handle) {
        this(methodName, execute, handle, null);
    }

    public SimpleFrame(String methodName, FrameCall<SimpleFrame<O>> execute, FrameCall<Throwable> handle,
                       FrameCall<Void> doFinally) {
        super(methodName);
        this.execute = execute;
        this.handle = handle;
        this.doFinally = doFinally;
    }

    @Override
    public FrameCallResult execute(Void input) throws Throwable {
        if (execute != null) {
            execute.execute(this);
        }
        return Fiber.frameReturn();
    }

    @Override
    protected FrameCallResult handle(Throwable ex) throws Throwable {
        if (handle != null) {
            handle.execute(ex);
            return Fiber.frameReturn();
        }
        throw ex;
    }

    @Override
    protected FrameCallResult doFinally() throws Throwable {
        if (doFinally != null) {
            doFinally.execute(null);
        }
        return Fiber.frameReturn();
    }

    @Override
    public FiberGroup getFiberGroup() {
        return super.getFiberGroup();
    }

    @Override
    public boolean isGroupShouldStopPlain() {
        return super.isGroupShouldStopPlain();
    }

    @Override
    public void setResult(O result) {
        super.setResult(result);
    }

    @Override
    public FrameCallResult justReturn(O result) {
        return super.justReturn(result);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + methodName + "@" + Integer.toHexString(hashCode());
    }
}
