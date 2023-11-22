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
public class FiberFuture<T> extends WaitSource {

    private boolean done;

    T result;
    Throwable execEx;

    FiberFuture(FiberGroup group) {
        super(group);
    }

    @Override
    protected boolean shouldWait(Fiber currentFiber) {
        return !done;
    }

    public T getResult() {
        return result;
    }

    public Throwable getEx() {
        return execEx;
    }

    public boolean isDone() {
        return done;
    }

    public boolean isCancelled() {
        return execEx instanceof FiberCancelException;
    }

    public void cancel() {
        completeExceptionally(new FiberCancelException());
    }

    public boolean isCanceled() {
        return execEx instanceof FiberCancelException;
    }

    public void complete(T result) {
        if (done) {
            return;
        }
        group.dispatcher.doInDispatcherThread(() -> complete0(result, null));
    }

    public void completeExceptionally(Throwable ex) {
        if (done) {
            return;
        }
        group.dispatcher.doInDispatcherThread(() -> complete0(null, ex));
    }

    private void complete0(T result, Throwable ex) {
        if (done) {
            return;
        }
        if (ex == null) {
            this.result = result;
        } else {
            this.execEx = ex;
        }
        this.done = true;
        signalAll0();
    }

    public FrameCallResult awaitOn(FrameCall<T> resumePoint) {
        return Dispatcher.awaitOn(this, 0, resumePoint);
    }

    public FrameCallResult awaitOn(long millis, FrameCall<T> resumePoint) {
        return Dispatcher.awaitOn(this, millis, resumePoint);
    }

    public FiberFrame<T> toFrame(long timeTimeoutMillis) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return awaitOn(timeTimeoutMillis, this::resume);
            }

            private FrameCallResult resume(T t) {
                setResult(t);
                return Fiber.frameReturn();
            }
        };
    }

    /**
     * this method can call in any thread
     */
    public void registerCallback(FiberFrame<Void> callbackFiberEntryFrame) {
        Fiber f = new Fiber("future-callback", group, callbackFiberEntryFrame);
        f.source = this;
        f.start();
    }

    /**
     * this method can call in any thread
     */
    public void registerCallback(FrameCall<Void> callbackFiberEntryFrameBody) {
        registerCallback(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                return callbackFiberEntryFrameBody.execute(null);
            }
        });
    }

    public static <T> FiberFuture<T> failedFuture(FiberGroup group, Throwable ex) {
        FiberFuture<T> f = new FiberFuture<>(group);
        f.done = true;
        f.execEx = ex;
        return f;
    }

    public static <T> FiberFuture<T> completedFuture(FiberGroup group, T result) {
        FiberFuture<T> f = new FiberFuture<>(group);
        f.done = true;
        f.result = result;
        return f;
    }
}
