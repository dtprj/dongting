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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author huangli
 */
public class FiberFuture<T> extends WaitSource {

    private static final DtLog log = DtLogs.getLogger(FiberFuture.class);

    private boolean done;

    T execResult;
    Throwable execEx;

    private Callback<T> callbackHead;

    FiberFuture(String name, FiberGroup group) {
        super(name, group);
    }

    @Override
    public String toString() {
        return "Future:" + name + "@" + Integer.toHexString(hashCode());
    }

    @Override
    protected void prepare(Fiber currentFiber, boolean timeout) {
        if (timeout) {
            currentFiber.inputEx = new FiberTimeoutException("wait "
                    + currentFiber.source + " timeout:" + currentFiber.scheduleTimeoutMillis + "ms");
            currentFiber.stackTop.resumePoint = null;
        } else {
            if (execEx != null) {
                currentFiber.inputEx = execEx;
                currentFiber.stackTop.resumePoint = null;
            } else {
                currentFiber.inputObj = execResult;
            }
        }
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

    public void complete(T result) {
        fiberGroup.checkGroup();
        complete0(result, null);
    }

    public void completeExceptionally(Throwable ex) {
        fiberGroup.checkGroup();
        complete0(null, ex);
    }

    public void fireComplete(T r) {
        fireComplete0(r, null);
    }

    public void fireCompleteExceptionally(Throwable ex) {
        fireComplete0(null, ex);
    }

    private void fireComplete0(T r, Throwable ex) {
        DispatcherThread dispatcherThread = fiberGroup.dispatcher.thread;
        if (Thread.currentThread() == dispatcherThread) {
            if (fiberGroup.finished) {
                log.warn("group is stopped, ignore fireComplete");
                return;
            }
            if (dispatcherThread.currentGroup == fiberGroup) {
                complete0(r, ex);
            } else {
                fiberGroup.sysChannel.offer0(() -> complete0(r, ex));
            }
        } else {
            if (!fiberGroup.sysChannel.fireOffer(() -> complete0(r, ex))) {
                log.warn("dispatcher is shutdown, ignore fireComplete");
            }
        }
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
        if (fiberGroup.finished) {
            return;
        }
        tryRunCallbacks();
        signalAll0(true);
    }

    private void tryRunCallbacks() {
        if (callbackHead == null) {
            return;
        }
        Callback<T> c = callbackHead;
        while (c != null) {
            if (c.frameCallback != null) {
                startCallbackFiber(c.frameCallback);
            } else {
                runSimpleCallback(c.simpleCallback);
            }
            c = c.next;
        }
    }

    private void runSimpleCallback(BiConsumer<T, Throwable> simpleCallback) {
        try {
            if (execEx == null) {
                simpleCallback.accept(execResult, null);
            } else {
                simpleCallback.accept(null, execEx);
            }
        } catch (Throwable e) {
            log.error("callback error", e);
        }
    }

    private void startCallbackFiber(FiberFrame<Void> ff) {
        Fiber f = new Fiber("future-callback", fiberGroup, ff);
        fiberGroup.start(f, true);
    }

    public FrameCallResult await(FrameCall<T> resumePoint) {
        if (done) {
            if (execEx == null) {
                return Fiber.resume(execResult, resumePoint);
            } else {
                return Fiber.resumeEx(execEx);
            }
        }
        return Dispatcher.awaitOn(this, -1, resumePoint);
    }

    public FrameCallResult await(long millis, FrameCall<T> resumePoint) {
        DtUtil.checkPositive(millis, "millis");
        if (done) {
            if (execEx == null) {
                return Fiber.resume(execResult, resumePoint);
            } else {
                return Fiber.resumeEx(execEx);
            }
        }
        return Dispatcher.awaitOn(this, millis, resumePoint);
    }

    /**
     * this method should call in dispatcher thread
     */
    public void registerCallback(FutureCallback<T> callback) {
        fiberGroup.checkGroup();
        if (done) {
            startCallbackFiber(callback);
        } else {
            Callback<T> c = new Callback<>();
            c.frameCallback = callback;
            addCallback(c);
        }
    }

    public abstract static class FutureCallback<T> extends FiberFrame<Void> {

        private final FiberFuture<T> future;

        public FutureCallback(FiberFuture<T> future) {
            this.future = future;
        }

        @Override
        public final FrameCallResult execute(Void input) throws Throwable {
            if (future.execEx != null) {
                throw future.execEx;
            } else {
                return afterCompleteSuccessfully(future.execResult);
            }
        }

        protected abstract FrameCallResult afterCompleteSuccessfully(T t);
    }

    private void addCallback(Callback<T> c) {
        if (callbackHead == null) {
            callbackHead = c;
            c.tail = c;
        } else {
            callbackHead.tail.next = c;
            callbackHead.tail = c;
        }
    }

    public void registerCallback(BiConsumer<T, Throwable> callback) {
        fiberGroup.checkGroup();
        if (done) {
            runSimpleCallback(callback);
        } else {
            Callback<T> c = new Callback<>();
            c.simpleCallback = callback;
            addCallback(c);
        }
    }

    private static class Callback<T> {
        FiberFrame<Void> frameCallback;
        BiConsumer<T, Throwable> simpleCallback;
        Callback<T> next;
        Callback<T> tail;
    }

    /**
     * this method should call in dispatcher thread.
     * NOTICE: if the future is complete exceptionally, the converter WILL NOT be called,
     * and the new future will be complete exceptionally with the same exception.
     */
    public <T2> FiberFuture<T2> convert(String name, Function<T, T2> converter) {
        FiberFuture<T2> newFuture = new FiberFuture<>(name, fiberGroup);
        registerCallback((r, ex) -> {
            if (ex != null) {
                newFuture.complete0(null, ex);
            } else {
                newFuture.complete0(converter.apply(r), null);
            }
        });
        return newFuture;
    }

    /**
     * this method should call in dispatcher thread
     */
    public <T2> FiberFuture<T2> convertWithHandle(String name, BiFunction<T, Throwable, T2> converter) {
        FiberFuture<T2> newFuture = new FiberFuture<>(name, fiberGroup);
        registerCallback((r, ex) -> {
            try {
                T2 t2 = converter.apply(r, ex);
                newFuture.complete(t2);
            } catch (Throwable newEx) {
                newFuture.completeExceptionally(newEx);
            }
        });
        return newFuture;
    }

    public static FiberFuture<Void> allOf(String name, FiberFuture<?>... futures) {
        FiberGroup g = FiberGroup.currentGroup();
        FiberFuture<Void> newFuture = g.newFuture(name);
        Fiber f = new Fiber("wait-all-future", g, new FiberFrame<Void>() {
            private int i;

            @Override
            public FrameCallResult execute(Void input) {
                return loop(null);
            }

            public FrameCallResult loop(Object unused) {
                if (i < futures.length) {
                    return futures[i++].await(this::loop);
                } else {
                    newFuture.complete(null);
                    return Fiber.frameReturn();
                }
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                newFuture.completeExceptionally(ex);
                return Fiber.frameReturn();
            }
        });
        g.start(f, false);
        return newFuture;
    }

    public static <T> FiberFuture<T> failedFuture(FiberGroup group, Throwable ex) {
        FiberFuture<T> f = new FiberFuture<>("FailedFuture", group);
        f.done = true;
        f.execEx = ex;
        return f;
    }

    public static <T> FiberFuture<T> completedFuture(FiberGroup group, T result) {
        FiberFuture<T> f = new FiberFuture<>("CompletedFuture", group);
        f.done = true;
        f.execResult = result;
        return f;
    }
}
