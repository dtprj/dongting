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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
class GroupExecutor implements ScheduledExecutorService {

    private final FiberGroup group;

    public GroupExecutor(FiberGroup group) {
        this.group = group;
    }

    @Override
    public void execute(Runnable command) {
        boolean b = group.sysChannel.fireOffer(command);
        if (!b) {
            throw new RejectedExecutionException("group or dispatcher is shutdown");
        }
    }

    private void submit0(Runnable task) {
        boolean b = group.sysChannel.fireOffer(task);
        if (!b) {
            throw new RejectedExecutionException("group or dispatcher is shutdown");
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        submit0(() -> {
            try {
                future.complete(task.call());
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @Override
    public Future<?> submit(Runnable task) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        submit0(() -> {
            try {
                task.run();
                future.complete(null);
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        CompletableFuture<T> future = new CompletableFuture<>();
        submit0(() -> {
            try {
                task.run();
                future.complete(result);
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @Override
    public boolean isShutdown() {
        return group.shareStatusSource.getShareStatus(false).shouldStop;
    }

    @Override
    public boolean isTerminated() {
        return group.finished;
    }

    static class SF<T> extends FiberFrame<Void> implements ScheduledFuture<T> {

        private final Runnable r;
        private final Callable<T> c;

        private final long initDelayNanos;
        private final long delayNanos;
        private final boolean fixedRate;
        private long nextRunTimeNanos;

        private final ReentrantLock lock;
        private Condition condition;
        private T result;
        private Throwable ex;

        private volatile int state = INIT;

        private static final int INIT = 0;
        private static final int FINISHED = 1;
        private static final int CANCEL = 2;

        public SF(Runnable r, Callable<T> c, long initDelayNanos, long delayNanos, boolean fixedRate) {
            this.initDelayNanos = initDelayNanos;
            this.delayNanos = delayNanos;
            this.fixedRate = fixedRate;
            this.r = r;
            this.c = c;
            this.lock = new ReentrantLock();
            this.nextRunTimeNanos = System.nanoTime() + initDelayNanos;
        }

        @Override
        public FrameCallResult execute(Void v) throws Throwable {
            if (initDelayNanos <= 0) {
                return executeOnce(v);
            }
            return Dispatcher.sleep(initDelayNanos, this::executeOnce);
        }

        private FrameCallResult executeOnce(Void v) {
            if (state == CANCEL) {
                return Fiber.frameReturn();
            }
            Timestamp ts = fiber.group.dispatcher.ts;
            long n = ts.nanoTime;
            run();
            if (delayNanos > 0) {
                ts.refresh(1);
                if (fixedRate) {
                    nextRunTimeNanos = n + delayNanos;
                    n = nextRunTimeNanos - ts.nanoTime;
                } else {
                    nextRunTimeNanos = ts.nanoTime + delayNanos;
                    n = delayNanos;
                }
                if (n > 0) {
                    return Dispatcher.sleep(n, this::executeOnce);
                } else {
                    return Fiber.yield(this::executeOnce);
                }
            }
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult handle(Throwable ex) throws Throwable {
            if (state != CANCEL) {
                this.ex = ex;
            }
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult doFinally() {
            finish(false);
            return Fiber.frameReturn();
        }

        private void run() {
            try {
                if (r != null) {
                    r.run();
                } else {
                    result = c.call();
                }
            } catch (Throwable e) {
                ex = e;
            }
        }

        private boolean finish(boolean cancel) {
            if (state != INIT) {
                return false;
            }
            lock.lock();
            try {
                if (state == INIT) {
                    if (cancel) {
                        ex = new CancellationException();
                        state = CANCEL;
                    } else {
                        state = FINISHED;
                    }
                    if (condition != null) {
                        condition.signalAll();
                    }
                    return true;
                } else {
                    return false;
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public long getDelay(TimeUnit unit) {
            if (state == CANCEL) {
                return 0;
            }
            return unit.convert(nextRunTimeNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (state != INIT) {
                return false;
            }
            if (finish(true)) {
                fiber.group.sysChannel.fireOffer(() -> fiber.interrupt());
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean isCancelled() {
            return state == CANCEL;
        }

        @Override
        public boolean isDone() {
            return state != INIT;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            lock.lock();
            try {
                if (condition == null) {
                    condition = lock.newCondition();
                }
                while (state == INIT) {
                    condition.await();
                }
                if (ex != null) {
                    throw new ExecutionException(ex);
                } else {
                    return result;
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            lock.lock();
            try {
                long nanos = unit.toNanos(timeout);
                if (condition == null) {
                    condition = lock.newCondition();
                }
                while (state == INIT) {
                    if (nanos <= 0) {
                        throw new TimeoutException();
                    }
                    nanos = condition.awaitNanos(nanos);
                }
                if (ex != null) {
                    throw new ExecutionException(ex);
                } else {
                    return result;
                }
            } finally {
                lock.unlock();
            }
        }
    }


    private <V> ScheduledFuture<V> doSchedule(SF<V> sf) {
        Fiber fiber = new Fiber("schedule-task", group, sf, sf.delayNanos != 0);
        if (group.fireFiber(fiber)) {
            return sf;
        } else {
            throw new RejectedExecutionException("group or dispatcher is shutdown");
        }
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        DtUtil.checkNotNegative(delay, "delay");
        return doSchedule(new SF<Void>(command, null, unit.toNanos(delay), 0, false));
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        DtUtil.checkNotNegative(delay, "delay");
        return doSchedule(new SF<>(null, callable, unit.toNanos(delay), 0, false));
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        DtUtil.checkNotNegative(initialDelay, "initialDelay");
        DtUtil.checkPositive(period, "period");
        return doSchedule(new SF<Void>(command, null, unit.toNanos(initialDelay), unit.toNanos(period), true));
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        DtUtil.checkNotNegative(initialDelay, "initialDelay");
        DtUtil.checkPositive(delay, "delay");
        return doSchedule(new SF<Void>(command, null, unit.toNanos(initialDelay), unit.toNanos(delay), false));
    }

    // -----------------------------------------------------------------------------

    private UnsupportedOperationException ex() {
        UnsupportedOperationException e = new UnsupportedOperationException();
        BugLog.getLog().error("unsupported operation", e);
        return e;
    }

    @Override
    public void shutdown() {
        throw ex();
    }

    @Override
    public List<Runnable> shutdownNow() {
        throw ex();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        throw ex();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
        throw ex();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
        throw ex();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
        throw ex();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
        throw ex();
    }


}
