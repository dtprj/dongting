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

    // use by FiberFuture
    T result;
    Throwable execEx;

    FiberFuture(FiberGroup group) {
        super(group);
    }

    public T getResult() {
        return result;
    }

    private void checkDone() {
        if (done) {
            throw new FiberException("already done");
        }
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

}
