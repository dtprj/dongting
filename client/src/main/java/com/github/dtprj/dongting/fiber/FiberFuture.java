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

import com.github.dtprj.dongting.common.RunnableEx;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.LinkedList;

/**
 * @author huangli
 */
public class FiberFuture<T> extends WaitSource {
    private static final DtLog log = DtLogs.getLogger(FiberFuture.class);

    private boolean done;

    T result;
    Throwable execEx;

    private LinkedList<RunnableEx<Throwable>> callbacks;

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
        if (callbacks != null) {
            for (RunnableEx<Throwable> r : callbacks) {
                try {
                    r.run();
                } catch (Throwable e) {
                    log.error("callback error in future {}", this, e);
                }
            }
        }
        signalAll0();
    }

    public FrameCallResult awaitOn(FrameCall<T> resumePoint) {
        return Dispatcher.awaitOn(this, 0, resumePoint);
    }

    public FrameCallResult awaitOn(long millis, FrameCall<T> resumePoint) {
        return Dispatcher.awaitOn(this, millis, resumePoint);
    }

    public FiberFrame<T> toFrame(long timeTimeoutMillis) {
        return new FiberFrame<T>() {
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

    public void registerCallback(RunnableEx<Throwable> r) {
        if (!group.isInGroupThread()) {
            throw new FiberException("register callback must be in group thread");
        }
        if (callbacks == null) {
            callbacks = new LinkedList<>();
        }
        callbacks.add(r);
    }
}
