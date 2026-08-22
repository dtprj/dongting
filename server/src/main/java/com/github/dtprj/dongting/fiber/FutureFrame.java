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
public class FutureFrame<O> extends FiberFrame<Void> {

    private final FiberFrame<O> sub;
    private final FiberFuture<O> future;

    public FutureFrame(FiberFuture<O> future, FiberFrame<O> sub) {
        this.sub = sub;
        this.future = future;
    }

    @Override
    public FrameCallResult execute(Void input) throws Throwable {
        return Fiber.call(sub, this::afterExec);
    }

    private FrameCallResult afterExec(O o) {
        future.complete(o);
        return Fiber.frameReturn();
    }

    @Override
    protected FrameCallResult handle(Throwable ex) throws Throwable {
        future.completeExceptionally(ex);
        return Fiber.frameReturn();
    }

    public static <O> FiberFuture<O> startWaitFiber(String name, FiberGroup g, FiberFrame<O> sub) {
        FiberFuture<O> f = g.newFuture(name);
        Fiber ff = new Fiber(name, g, new FutureFrame<>(f, sub));
        ff.start();
        return f;
    }
}
