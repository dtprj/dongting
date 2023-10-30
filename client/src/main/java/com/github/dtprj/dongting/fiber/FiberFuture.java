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
public class FiberFuture<T> {
    private T result;
    private Throwable ex;

    private boolean done;
    private final Fiber fiber;

    FiberFuture(Fiber fiber) {
        this.fiber = fiber;
    }

    public T getResult() {
        return result;
    }

    public Throwable getEx() {
        return ex;
    }

    public void complete(T result) {
        if (done) {
            return;
        }
        this.result = result;
        this.done = true;
        if (fiber != null) {
            fiber.getGroup().makeReady(fiber);
        }
    }

    public void completeExceptionally(Throwable ex) {
        if (done) {
            return;
        }
        this.ex = ex;
        this.done = true;
        if (fiber != null) {
            fiber.getGroup().makeReady(fiber);
        }
    }

    public Fiber getFiber() {
        return fiber;
    }
}
