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
        if (Thread.currentThread() == fiberGroup.dispatcher.thread) {
            completeOutOfGroup(r, ex);
        } else {
            boolean b = fiberGroup.dispatcher.doInDispatcherThread(new FiberQueueTask() {
                @Override
                protected void run() {
                    completeOutOfGroup(r, ex);
                }
            });
            if (!b) {
                log.info("dispatcher is shutdown, ignore fireComplete");
            }
        }
    }

    private void completeOutOfGroup(T r, Throwable ex){
        if (fiberGroup.finished) {
            log.info("group {} is finished, ignore future complete", fiberGroup.getName());
            return;
        }
        complete0(r, ex);
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
        signalAll0(true);
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
    private void registerCallback0(FiberFrame<Void> callback) {
        fiberGroup.checkGroup();
        Fiber callbackFiber = new Fiber("future-callback", fiberGroup, callback);
        callbackFiber.start();
    }

    /**
     * this method should call in dispatcher thread
     */
    public void registerCallback(FutureCallback<T> callback) {
        callback.future = this;
        registerCallback0(callback);
    }

    public static abstract class FutureCallback<T> extends FiberFrame<Void> {

        private FiberFuture<T> future;

        @Override
        public final FrameCallResult execute(Void input) {
            return Fiber.call(new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    return future.await(this::afterFutureCompleted);
                }
                private FrameCallResult afterFutureCompleted(T t) {
                    return Fiber.frameReturn();
                }
                @Override
                protected FrameCallResult handle(Throwable ex) {
                    // catch ex
                    return Fiber.frameReturn();
                }
            }, this::resume);
        }

        private FrameCallResult resume(Void v) {
            return onCompleted(future.getResult(), future.getEx());
        }

        protected abstract FrameCallResult onCompleted(T t, Throwable ex);
    }

    /**
     * this method should call in dispatcher thread
     */
    public <T2> FiberFuture<T2> convert(Function<T, T2> converter) {
        FiberFuture<T2> newFuture = new FiberFuture<>(fiberGroup);
        registerCallback0(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                FiberFuture<T> f = FiberFuture.this;
                return f.await(this::resume);
            }

            private FrameCallResult resume(T t) {
                newFuture.complete(converter.apply(t));
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                // if the ex is thrown by converter, completeExceptionally with the ex is ok
                newFuture.completeExceptionally(ex);
                return Fiber.frameReturn();
            }
        });
        return newFuture;
    }

    /**
     * this method should call in dispatcher thread
     */
    public <T2> FiberFuture<T2> convertWithHandle(BiFunction<T, Throwable, T2> converter) {
        FiberFuture<T2> newFuture = new FiberFuture<>(fiberGroup);
        registerCallback(new FutureCallback<>() {
            @Override
            protected FrameCallResult onCompleted(T t, Throwable ex) {
                try {
                    T2 t2 = converter.apply(t, ex);
                    newFuture.complete(t2);
                } catch (Throwable newEx) {
                    newFuture.completeExceptionally(newEx);
                }
                return Fiber.frameReturn();
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
