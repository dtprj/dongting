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
package com.github.dtprj.dongting.raft.test;

import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;

import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class FiberUtil {
    public static <T> CompletableFuture<T> toJdkFuture(FiberFuture<T> fiberFuture) {
        CompletableFuture<T> future = new CompletableFuture<>();
        fiberFuture.getGroup().fireFiber("toJdkFuture", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return fiberFuture.await(this::resume);
            }

            private FrameCallResult resume(T t) {
                future.complete(t);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                future.completeExceptionally(ex);
                return Fiber.frameReturn();
            }
        });
        return future;
    }
}
