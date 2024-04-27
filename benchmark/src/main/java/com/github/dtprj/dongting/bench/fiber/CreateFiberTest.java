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
package com.github.dtprj.dongting.bench.fiber;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.fiber.Dispatcher;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;

import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class CreateFiberTest {
    private static final int LOOP = 10_000_000;
    private static final boolean FIRE_FIBER_IN_GROUP = false;
    private int count;
    private long startTime;

    private final Dispatcher dispatcher = new Dispatcher("testDispatcher");
    private final FiberGroup group = new FiberGroup("testGroup", dispatcher);

    public static void main(String[] args) {
        CreateFiberTest t = new CreateFiberTest();
        t.run();
        t.dispatcher.stop(new DtTime(10, TimeUnit.SECONDS));
    }

    CreateFiberTest() {
        dispatcher.start();
        dispatcher.startGroup(group).join();
    }

    private void run() {
        if (FIRE_FIBER_IN_GROUP) {
            group.fireFiber("master", new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    startTime = System.currentTimeMillis();
                    for (int i = 0; i < LOOP; i++) {
                        createTask().start();
                    }
                    return Fiber.frameReturn();
                }
            });
        } else {
            startTime = System.currentTimeMillis();
            for (int i = 0; i < LOOP; i++) {
                group.fireFiber(createTask());
            }
        }
    }

    private Fiber createTask() {
        return new Fiber("worker", group, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (++count == LOOP) {
                    System.out.println("Time: " + (System.currentTimeMillis() - startTime) + "ms");
                }
                return Fiber.frameReturn();
            }
        });
    }

}

//public class CreateVirtualThreadTest {
//
//    private static final int LOOP = 10_000_000;
//    private int count;
//    private ExecutorService executor;
//    private long startTime;
//
//    public static void main(String[] args) throws InterruptedException {
//        new CreateVirtualThreadTest().run();
//    }
//
//    public void run() throws InterruptedException {
//        executor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
//        startTime = System.currentTimeMillis();
//        for (int i = 0; i < LOOP; i++) {
//            executor.submit(this::startFiber);
//        }
//        executor.awaitTermination(10, TimeUnit.SECONDS);
//    }
//
//    private void startFiber() {
//        if (++count == LOOP) {
//            System.out.println("Time: " + (System.currentTimeMillis() - startTime) + "ms");
//            executor.shutdown();
//        }
//    }
//
//}
