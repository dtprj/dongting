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
import com.github.dtprj.dongting.common.RunnableEx;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
class AbstractFiberTest {
    private static final DtLog log = DtLogs.getLogger(AbstractFiberTest.class);

    protected Dispatcher dispatcher;
    protected FiberGroup fiberGroup;

    @BeforeEach
    public void initGroup() throws Exception {
        dispatcher = new Dispatcher("test");
        dispatcher.start();
        fiberGroup = new FiberGroup("test group", dispatcher);
        dispatcher.startGroup(fiberGroup).get();
    }

    static void shutdownDispatcher(Dispatcher dispatcher, FiberGroup fiberGroup) throws Exception {
        if (dispatcher.thread.isAlive()) {
            dispatcher.doInDispatcherThread(new FiberQueueTask(null) {
                @Override
                protected void run() {
                    // fix time if it's updated by TestUtil.updateTimestamp()
                    TestUtil.updateTimestamp(dispatcher.getTs(), System.nanoTime(), System.currentTimeMillis());
                    dispatcher.stop(new DtTime(1000, TimeUnit.MILLISECONDS));
                }
            });
            dispatcher.thread.join(1500);
            if (dispatcher.thread.isAlive()) {
                fail();
            }
            assertTrue(fiberGroup.finished);
        }
    }

    @AfterEach
    public void shutdownDispatcher() throws Exception {
        shutdownDispatcher(dispatcher, fiberGroup);
    }

    static void doInFiber(FiberGroup fiberGroup, FiberFrame<Void> fiberFrame) throws Exception {
        CompletableFuture<Void> f = new CompletableFuture<>();
        fiberGroup.fireFiber("do-in-fiber", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(fiberFrame, this::resume);
            }

            private FrameCallResult resume(Void unused) {
                f.complete(null);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                f.completeExceptionally(ex);
                return Fiber.frameReturn();
            }
        });
        try {
            f.get(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("doInFiber timeout(5s)", e);
            fiberGroup.fireLogGroupInfo("doInFiber timeout(5s)");
            throw e;
        }
    }

    protected void doInFiber(FiberFrame<Void> fiberFrame) throws Exception {
        doInFiber(fiberGroup, fiberFrame);
    }

    static void doInFiber(FiberGroup fiberGroup, RunnableEx<Throwable> callback) throws Exception {
        CompletableFuture<Void> f = new CompletableFuture<>();
        fiberGroup.fireFiber("do-in-fiber", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Throwable {
                callback.run();
                f.complete(null);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                f.completeExceptionally(ex);
                return Fiber.frameReturn();
            }
        });
        try {
            f.get(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("doInFiber timeout(5s)", e);
            fiberGroup.fireLogGroupInfo("doInFiber timeout(5s)");
            throw e;
        }
    }

    protected void doInFiber(RunnableEx<Throwable> callback) throws Exception {
        doInFiber(fiberGroup, callback);
    }
}
