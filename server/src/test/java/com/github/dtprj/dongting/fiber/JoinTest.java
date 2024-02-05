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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class JoinTest extends AbstractFiberTest {
    @Test
    public void testJoin1() throws Exception {
        testJoinImpl(1);
    }

    @Test
    public void testJoin2() throws Exception {
        testJoinImpl(2);
    }

    @Test
    public void testJoin3() throws Exception {
        testJoinImpl(3);
    }

    private void testJoinImpl(int joinType) throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Fiber f = new Fiber("f1", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.sleep(1000, this::justReturn);
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(f);
        fiberGroup.fireFiber("f2", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                f.interrupt();
                if (joinType == 1) {
                    return f.join(this::resume);
                } else if (joinType == 2) {
                    return f.join(1000, this::resume2);
                } else {
                    FiberFuture<Void> joinFuture = f.join();
                    return joinFuture.await(this::resume);
                }
            }

            private FrameCallResult resume(Void unused) {
                Assertions.assertTrue(f.finished);
                future.complete(null);
                return Fiber.frameReturn();
            }

            private FrameCallResult resume2(Boolean ok) {
                Assertions.assertTrue(ok);
                Assertions.assertTrue(f.finished);
                future.complete(null);
                return Fiber.frameReturn();
            }
        });
        future.get(1, TimeUnit.SECONDS);
    }
}
