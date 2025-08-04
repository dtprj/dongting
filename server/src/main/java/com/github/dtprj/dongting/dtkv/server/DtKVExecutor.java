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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
class DtKVExecutor {
    private final ScheduledExecutorService separateExecutor;
    private final FiberGroup fiberGroup;
    final Timestamp ts;

    DtKVExecutor(Timestamp ts, ScheduledExecutorService separateExecutor, FiberGroup fiberGroup) {
        this.ts = ts;
        this.separateExecutor = separateExecutor;
        this.fiberGroup = fiberGroup;
        if (separateExecutor != null) {
            separateExecutor.scheduleWithFixedDelay(ts::refresh, 1, 1, TimeUnit.MILLISECONDS);
        }
    }

    public void submitTaskInAnyThread(Runnable r) throws RejectedExecutionException {
        Executor e = separateExecutor == null ? fiberGroup.getExecutor() : separateExecutor;
        e.execute(r);
    }

    public void submitTaskInFiberThread(Runnable r) {
        if (separateExecutor == null) {
            r.run();
        } else {
            separateExecutor.execute(r);
        }
    }

    public void startDaemonTask(String name, DtKVExecutorTask task) {
        if (separateExecutor == null) {
            task.cond = fiberGroup.newCondition("cond-" + name);
            Fiber f = new Fiber("fiber-" + name, fiberGroup, new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    long nextDelayNanos = task.executeTaskOnce();
                    if (nextDelayNanos == 0) {
                        return Fiber.yield(this);
                    } else if (nextDelayNanos > 0) {
                        return task.cond.await(nextDelayNanos, TimeUnit.NANOSECONDS, this);
                    } else {
                        return Fiber.frameReturn();
                    }
                }
            }, true);
            f.start();
        } else {
            separateExecutor.execute(task);
        }
    }

    /**
     * this method should be called in the executor(fiber thread or separateExecutor thread).
     */
    public void signalTask(DtKVExecutorTask task) {
        if (task.cond != null) {
            task.cond.signal();
        } else {
            if (task.future != null) {
                task.future.cancel(false);
                task.future = null;
                separateExecutor.execute(task);
            }
        }
    }

    abstract class DtKVExecutorTask implements Runnable {

        private FiberCondition cond;
        private ScheduledFuture<?> future;

        public void run() {
            future = null;
            long nextDelayNanos = executeTaskOnce();
            if (nextDelayNanos == 0) {
                separateExecutor.execute(this);
            } else if (nextDelayNanos > 0) {
                future = separateExecutor.schedule(this, nextDelayNanos, TimeUnit.NANOSECONDS);
            }
        }

        private long executeTaskOnce() {
            if (shouldStop()) {
                return -1;
            }
            if (shouldPause()) {
                // at least 1ms
                return Math.max(1_000_000L, defaultDelayNanos());
            }
            long nextDelayNanos = 0;
            boolean fail = false;
            try {
                nextDelayNanos = execute();
            } catch (Exception e) {
                fail = true;
                BugLog.log(e);
            }
            return Math.max(0, fail ? defaultDelayNanos() : nextDelayNanos);
        }

        protected abstract long execute();

        protected abstract boolean shouldPause();

        protected abstract boolean shouldStop();

        protected abstract long defaultDelayNanos();
    }
}


