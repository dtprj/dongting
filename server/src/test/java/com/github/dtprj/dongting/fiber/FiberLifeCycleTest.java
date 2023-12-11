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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class FiberLifeCycleTest {
    private Dispatcher dispatcher;
    private FiberGroup group;

    @BeforeEach
    public void setup() throws Exception {
        dispatcher = new Dispatcher("test");
        dispatcher.start();
        group = new FiberGroup("test group", dispatcher);
        dispatcher.startGroup(group).get();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (dispatcher.thread.isAlive()) {
            dispatcher.stop(new DtTime(1, TimeUnit.SECONDS));
            dispatcher.thread.join(1000);
            Assertions.assertFalse(dispatcher.thread.isAlive());
            assertTrue(group.finished);
        }
    }

    @Test
    public void testDispatcherStop() {
        // just test in setup() and tearDown()
    }

    @Test
    public void testDaemonFiber() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Fiber f = new Fiber("daemonFiber", group, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (countDownLatch.getCount() == 1) {
                    countDownLatch.countDown();
                }
                return Fiber.sleep(1, this);
            }
        }, true);
        group.fireFiber(f);
        // wait daemon fiber exec
        countDownLatch.await();
    }

    @Test
    public void testGroupShutdown() throws Exception {
        FiberGroup g2 = new FiberGroup("g2", dispatcher);
        dispatcher.startGroup(g2).get();
        group.requestShutdown();
        AtomicBoolean groupFinished = new AtomicBoolean();
        g2.fireFiber(new Fiber("f", g2, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (group.finished) {
                    groupFinished.set(true);
                    return Fiber.frameReturn();
                }
                return Fiber.sleep(1, this);
            }
        }));
        TestUtil.waitUtil(groupFinished::get);
    }

}
