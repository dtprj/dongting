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

    FiberFuture(FiberGroup group) {
        super(group);
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

    public boolean isCanceled() {
        return execEx instanceof FiberCancelException;
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
                log.info("group is stopped, ignore fireComplete");
                return;
            }
            if (dispatcherThread.currentGroup == fiberGroup) {
                complete0(r, ex);
            } else {
                fiberGroup.sysChannel.offer0(() -> {
                    if (fiberGroup.finished) {
                        log.info("group is stopped, ignore fireComplete");
                        return;
                    }
                    complete0(r, ex);
                });
            }
        } else {
            boolean b = fiberGroup.sysChannel.fireOffer(() -> {
                if (fiberGroup.finished) {
                    log.info("group is stopped, ignore fireComplete");
                    return;
                }
                complete0(r, ex);
            });
            if (!b) {
                log.info("dispatcher is shutdown, ignore fireComplete");
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
        return await("waitOnFuture", resumePoint);
    }

    public FrameCallResult await(String reason, FrameCall<T> resumePoint) {
        if (done) {
            if (execEx == null) {
                return Fiber.resume(execResult, resumePoint);
            } else {
                return Fiber.resumeEx(execEx);
            }
        }
        return Dispatcher.awaitOn(this, -1, resumePoint, reason);
    }

    public FrameCallResult await(long millis, FrameCall<T> resumePoint) {
        return await(millis, "timeWaitOnFuture", resumePoint);
    }

    public FrameCallResult await(long millis, String reason, FrameCall<T> resumePoint) {
        DtUtil.checkPositive(millis, "millis");
        if (done) {
            if (execEx == null) {
                return Fiber.resume(execResult, resumePoint);
            } else {
                return Fiber.resumeEx(execEx);
            }
        }
        return Dispatcher.awaitOn(this, millis, resumePoint, reason);
    }

    /**
     * this method should call in dispatcher thread
     */
    public void registerCallback(FutureCallback<T> callback) {
        callback.future = this;
        registerCallback0(callback);
    }

    /**
     * this method should call in dispatcher thread
     */
    private void registerCallback0(FiberFrame<Void> callback) {
        fiberGroup.checkGroup();
        if (done) {
            startCallbackFiber(callback);
        } else {
            Callback<T> c = new Callback<>();
            c.frameCallback = callback;
            addCallback(c);
        }
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

    public static abstract class FutureCallback<T> extends FiberFrame<Void> {

        private FiberFuture<T> future;

        @Override
        public final FrameCallResult execute(Void input) {
            return onCompleted(future.getResult(), future.getEx());
        }

        protected abstract FrameCallResult onCompleted(T t, Throwable ex);
    }

    /**
     * this method should call in dispatcher thread
     */
    public <T2> FiberFuture<T2> convert(Function<T, T2> converter) {
        FiberFuture<T2> newFuture = new FiberFuture<>(fiberGroup);
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
    public <T2> FiberFuture<T2> convertWithHandle(BiFunction<T, Throwable, T2> converter) {
        FiberFuture<T2> newFuture = new FiberFuture<>(fiberGroup);
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

    public static FiberFuture<Void> allOf(FiberFuture<?>... futures) {
        FiberGroup g = FiberGroup.currentGroup();
        FiberFuture<Void> newFuture = g.newFuture();
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
