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
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
class DtKVExecutor {
    private static final DtLog log = DtLogs.getLogger(DtKVExecutor.class);
    protected final ScheduledExecutorService separateExecutor;
    private final FiberGroup fiberGroup;
    final Timestamp ts;

    DtKVExecutor(int groupId, Timestamp ts, FiberGroup fiberGroup) {
        this.ts = ts;
        if (fiberGroup == null) {
            this.separateExecutor = createExecutor(groupId);
        } else {
            this.separateExecutor = null;
        }
        this.fiberGroup = fiberGroup;
    }

    public boolean submitTaskInAnyThread(Runnable r) throws RejectedExecutionException {
        try {
            Executor e = separateExecutor == null ? fiberGroup.getExecutor() : separateExecutor;
            e.execute(r);
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    public boolean submitTaskInFiberThread(Runnable r) {
        if (separateExecutor == null) {
            r.run();
            return true;
        } else {
            try {
                separateExecutor.execute(r);
                return true;
            } catch (RejectedExecutionException e) {
                return false;
            }
        }
    }

    public void submitTaskInFiberThread(FiberFuture<?> f, Runnable r) {
        if (!submitTaskInFiberThread(r)) {
            f.completeExceptionally(new RaftException("stopped"));
        }
    }

    public boolean startDaemonTask(String name, DtKVExecutorTask task) {
        if (separateExecutor == null) {
            Fiber f = new Fiber("fiber-" + name, fiberGroup, new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    task.cond = fiberGroup.newCondition("cond-" + name);
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
            return fiberGroup.fireFiber(f);
        } else {
            try {
                task.executor = separateExecutor;
                separateExecutor.execute(task);
                return true;
            } catch (RejectedExecutionException e) {
                return false;
            }
        }
    }

    protected ScheduledExecutorService createExecutor(int groupId) {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("DtKV-" + groupId);
            return t;
        });
    }

    public void start() {
        if (separateExecutor != null) {
            separateExecutor.scheduleWithFixedDelay(ts::refresh, 1, 1, TimeUnit.MILLISECONDS);
        }
    }

    public void stop() {
        if (separateExecutor != null) {
            separateExecutor.shutdownNow();
        }
    }

    static abstract class DtKVExecutorTask implements Runnable {

        private ScheduledExecutorService executor;
        private FiberCondition cond;
        private ScheduledFuture<?> future;

        public final void run() {
            future = null;
            long nextDelayNanos = executeTaskOnce();
            if (shouldStop()) {
                return;
            }
            try {
                if (nextDelayNanos == 0) {
                    executor.execute(this);
                } else if (nextDelayNanos > 0) {
                    future = executor.schedule(this, nextDelayNanos, TimeUnit.NANOSECONDS);
                }
            } catch (RejectedExecutionException e) {
                log.warn("executor stopped, task submit failed");
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

        /**
         * this method should be called in the executor(fiber thread or separateExecutor thread).
         */
        public void signal() {
            if (cond == null) {
                cond.signal();
            } else {
                if (future != null) {
                    future.cancel(false);
                    future = null;
                    executor.execute(this);
                }
            }
        }

        protected abstract long execute();

        protected abstract boolean shouldPause();

        protected abstract boolean shouldStop();

        protected abstract long defaultDelayNanos();
    }
}


