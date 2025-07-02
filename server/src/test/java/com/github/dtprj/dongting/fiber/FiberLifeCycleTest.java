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

import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author huangli
 */
public class FiberLifeCycleTest extends AbstractFiberTest {

    @Test
    public void testDispatcherStop() {
        // just test in setup() and tearDown()
    }

    @Test
    public void testDaemonFiber() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Fiber f = new Fiber("daemonFiber", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (countDownLatch.getCount() == 1) {
                    countDownLatch.countDown();
                }
                return Fiber.sleep(1, this);
            }
        }, true);
        fiberGroup.fireFiber(f);
        // wait daemon fiber exec
        countDownLatch.await();
    }

    @Test
    public void testGroupShutdown() throws Exception {
        FiberGroup g2 = new FiberGroup("g2", dispatcher);
        dispatcher.startGroup(g2).get();
        fiberGroup.requestShutdown();
        AtomicBoolean groupFinished = new AtomicBoolean();
        g2.fireFiber(new Fiber("f", g2, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (FiberLifeCycleTest.this.fiberGroup.finished) {
                    groupFinished.set(true);
                    return Fiber.frameReturn();
                }
                return Fiber.sleep(1, this);
            }
        }));
        WaitUtil.waitUtil(groupFinished::get);
    }

}
