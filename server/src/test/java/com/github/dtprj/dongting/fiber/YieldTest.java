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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author huangli
 */
public class YieldTest extends AbstractFiberTest {
    @Test
    public void testYield() throws Exception {
        CompletableFuture<Void> f = new CompletableFuture<>();
        AtomicInteger count = new AtomicInteger();
        fiberGroup.fireFiber("f1", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (count.get() == 0) {
                    return Fiber.yield(this);
                }
                f.complete(null);
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber("f2", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                count.set(1);
                return Fiber.frameReturn();
            }
        });
        f.get(1, TimeUnit.SECONDS);
    }
}
