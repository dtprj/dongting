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

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * @author huangli
 */
public class FiberFuture<T> extends WaitSource {

    private boolean done;

    T execResult;
    Throwable execEx;

    FiberFuture(FiberGroup group) {
        super(group);
    }

    @Override
    protected boolean shouldWait(Fiber currentFiber) {
        return !done;
    }

    public T getResult() {
        return execResult;
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
        group.checkThread();
        complete0(result, null);
    }

    public void completeExceptionally(Throwable ex) {
        group.checkThread();
        complete0(null, ex);
    }

    public void fireComplete(T r) {
        fireComplete0(r, null);
    }

    public void fireCompleteExceptionally(Throwable ex) {
        fireComplete0(null, ex);
    }

    private void fireComplete0(T r, Throwable ex) {
        group.dispatcher.doInDispatcherThread(new FiberQueueTask() {
            @Override
            protected void run() {
                complete0(r, ex);
            }
        });
    }

    private void complete0(T result, Throwable ex) {
        if (done) {
            return;
        }
        if (ex == null) {
            this.execResult = result;
        } else {
            this.execEx = ex;
        }
        this.done = true;
        // if group finished, no ops
        signalAll0();
    }

    public FrameCallResult awaitOn(FrameCall<T> resumePoint) {
        return Dispatcher.awaitOn(this, 0, resumePoint);
    }

    public FrameCallResult awaitOn(long millis, FrameCall<T> resumePoint) {
        return Dispatcher.awaitOn(this, millis, resumePoint);
    }

    public FiberFrame<T> toFrame() {
        return toFrame(0);
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
     * this method should call in dispatcher thread
     */
    public void registerCallback(FiberFrame<Void> callbackFiberEntryFrame) {
        group.checkThread();
        Fiber f = new Fiber("future-callback", group, callbackFiberEntryFrame);
        f.source = this;
        f.start();
    }

    /**
     * this method should call in dispatcher thread
     */
    public void registerCallback(BiConsumer<T, Throwable> callback) {
        registerCallback(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                FiberFuture<T> f = FiberFuture.this;
                callback.accept(f.execResult, null);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                callback.accept(null, ex);
                return Fiber.frameReturn();
            }
        });
    }

    /**
     * this method should call in dispatcher thread
     */
    public <T2> FiberFuture<T2> convert(Function<T, T2> converter) {
        FiberFuture<T2> newFuture = new FiberFuture<>(group);
        registerCallback(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                FiberFuture<T> f = FiberFuture.this;
                T2 r2 = converter.apply(f.execResult);
                newFuture.complete(r2);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                newFuture.completeExceptionally(ex);
                return Fiber.frameReturn();
            }
        });
        return newFuture;
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
        f.execResult = result;
        return f;
    }
}
