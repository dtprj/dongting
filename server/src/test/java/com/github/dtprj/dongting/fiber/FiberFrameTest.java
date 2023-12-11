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

import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author huangli
 */
public class FiberFrameTest extends AbstractFiberTest {

    @Test
    public void testCall() {
        AtomicBoolean f1Called = new AtomicBoolean();
        AtomicBoolean f2Called = new AtomicBoolean();
        AtomicInteger f2Result = new AtomicInteger();
        FiberCondition c = group.newCondition();
        FiberFrame<Integer> f2 = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return c.awaitOn(this::resume);
            }

            private FrameCallResult resume(Void unused) {
                f2Called.set(true);
                setResult(100);
                return Fiber.frameReturn();
            }
        };

        FiberFrame<Void> f1 = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(f2, this::resume);
            }

            private FrameCallResult resume(Integer r) {
                f2Result.set(r);
                f1Called.set(true);
                return Fiber.frameReturn();
            }
        };

        Fiber fiber = new Fiber("fiber", group, f1);
        group.fireFiber(fiber);
        group.fireFiber(new Fiber("signal", group, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                c.signal();
                return Fiber.frameReturn();
            }
        }));
        TestUtil.waitUtil(f2Called::get);
        TestUtil.waitUtil(f1Called::get);
        Assertions.assertEquals(100, f2Result.get());
    }
}
