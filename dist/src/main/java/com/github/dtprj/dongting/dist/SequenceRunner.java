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
package com.github.dtprj.dongting.dist;

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * A thread-safe serialized task runner.
 * <p>
 * The task is idempotent/re-runnable, so multiple {@link #submit()} calls may be coalesced,
 * but the runner guarantees that at most one task execution is running at any time.
 * <p>
 * This class is NOT fiber-based.
 *
 * @author huangli
 */
public class SequenceRunner {

    private static final DtLog log = DtLogs.getLogger(SequenceRunner.class);

    private final Executor executor;
    private final Runnable task;

    private final int[] retryIntervals;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition doneCondition = lock.newCondition();

    private long requestVersion;
    private long finishedVersion;

    private boolean running;

    private int failCount;

    private LinkedList<CallbackEntry> callbacks;

    /**
     * @param executor executor to run task
     * @param task task to run, should be idempotent/re-runnable.
     * @param retryIntervals retry intervals in milliseconds, for example: {1000, 2000, 5000}.
     *                       When task throws, retry after 1000ms, then 2000ms, then always 5000ms.
     *                       Empty or null means no delay retry (retry immediately).
     */
    public SequenceRunner(Executor executor, Runnable task, int[] retryIntervals) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.task = Objects.requireNonNull(task, "task");
        if (retryIntervals != null && retryIntervals.length > 0) {
            int[] copy = retryIntervals.clone();
            for (int v : copy) {
                if (v < 0) {
                    throw new IllegalArgumentException("retryIntervals contains negative value");
                }
            }
            this.retryIntervals = copy;
        } else {
            this.retryIntervals = null;
        }
    }

    /**
     * Submit a new request, returns the version of this request.
     * <p>
     * This method never blocks on task execution.
     */
    public long submit() {
        return submit(0, null);
    }

    /**
     * Submit a new request with optional callback.
     * <p>
     * This method never blocks on task execution.
     *
     * @param callbackTimeoutMillis timeout in milliseconds for callback, ignored if callback is null.
     *                              Must be positive if callback is not null.
     * @param callback              optional callback to invoke when this request finishes, timeout or fails.
     *                              Receives null on success, TimeoutException on timeout, or task exception on failure.
     * @return the version of this request
     */
    public long submit(long callbackTimeoutMillis, Consumer<Throwable> callback) {
        if (callback != null && callbackTimeoutMillis <= 0) {
            throw new IllegalArgumentException("callbackTimeoutMillis must be positive when callback is not null");
        }
        long v;
        boolean needStart = false;
        CallbackEntry entry = null;
        lock.lock();
        try {
            v = ++requestVersion;
            if (!running) {
                running = true;
                needStart = true;
            }
            if (callback != null) {
                entry = new CallbackEntry();
                entry.callback = callback;
                entry.targetVersion = v;
                if (callbacks == null) {
                    callbacks = new LinkedList<>();
                }
                callbacks.add(entry);
            }
        } finally {
            lock.unlock();
        }
        if (needStart) {
            executor.execute(this::runOnce);
        }
        if (entry != null) {
            // schedule timeout outside lock
            CallbackEntry e = entry;
            e.timeoutFuture = DtUtil.SCHEDULED_SERVICE.schedule(
                    () -> onCallbackTimeout(e), callbackTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        return v;
    }

    public long getRequestVersion() {
        lock.lock();
        try {
            return requestVersion;
        } finally {
            lock.unlock();
        }
    }

    public long getFinishedVersion() {
        lock.lock();
        try {
            return finishedVersion;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Wait until current latest {@link #submit()} request finishes.
     *
     * @return true if reached within timeout, false if timeout.
     */
    public boolean awaitFinish(long timeoutMillis) throws InterruptedException {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeoutMillis < 0");
        }
        lock.lock();
        try {
            long version = requestVersion;
            if (finishedVersion >= version) {
                return true;
            }
            long nanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            while (finishedVersion < version) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = doneCondition.awaitNanos(nanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void runOnce() {
        long currentTaskVersion;
        lock.lock();
        try {
            if (finishedVersion >= requestVersion) {
                running = false;
                return;
            }
            currentTaskVersion = requestVersion;
        } finally {
            lock.unlock();
        }

        Throwable ex = null;
        try {
            task.run();
            failCount = 0;
        } catch (Throwable e) {
            ex = e;
            log.error("run task failed", e);
        }

        if (ex == null) {
            boolean shouldStop;
            LinkedList<CallbackEntry> toInvoke;
            lock.lock();
            try {
                if (finishedVersion < currentTaskVersion) {
                    finishedVersion = currentTaskVersion;
                }
                doneCondition.signalAll();
                shouldStop = finishedVersion >= requestVersion;
                if (shouldStop) {
                    running = false;
                }
                toInvoke = collectCallbacks(currentTaskVersion, null);
            } finally {
                lock.unlock();
            }
            invokeCallbacks(toInvoke);
            if (shouldStop) {
                return;
            }
            // still have pending requests, keep running in executor
            executor.execute(this::runOnce);
            return;
        }

        // failed
        if (retryIntervals == null) {
            // do not retry if no retry intervals configured
            boolean shouldStop;
            LinkedList<CallbackEntry> toInvoke;
            lock.lock();
            try {
                // don't update finishedVersion since task failed
                // check if new requests arrived during task execution
                shouldStop = currentTaskVersion >= requestVersion;
                if (shouldStop) {
                    running = false;
                }
                toInvoke = collectCallbacks(currentTaskVersion, ex);
            } finally {
                lock.unlock();
            }
            invokeCallbacks(toInvoke);
            if (shouldStop) {
                return;
            }
            // new requests arrived during task execution, continue running
            executor.execute(this::runOnce);
            return;
        }

        int delay = getRetryIntervalMillis(failCount++);
        if (delay <= 0) {
            executor.execute(this::runOnce);
        } else {
            DtUtil.SCHEDULED_SERVICE.schedule(() -> executor.execute(this::runOnce), delay, TimeUnit.MILLISECONDS);
        }
    }

    private int getRetryIntervalMillis(int failCount) {
        int[] arr = this.retryIntervals;
        if (arr == null) {
            return 0;
        }
        if (failCount < 0) {
            return arr[0];
        }
        if (failCount < arr.length) {
            return arr[failCount];
        }
        return arr[arr.length - 1];
    }

    private void onCallbackTimeout(CallbackEntry entry) {
        lock.lock();
        try {
            if (entry.done) {
                return;
            }
            entry.done = true;
            if (callbacks != null) {
                callbacks.remove(entry);
            }
        } finally {
            lock.unlock();
        }
        invokeCallback(entry.callback, new TimeoutException("callback timeout"));
    }

    // caller must hold lock
    private LinkedList<CallbackEntry> collectCallbacks(long version, Throwable ex) {
        if (callbacks == null || callbacks.isEmpty()) {
            return null;
        }
        LinkedList<CallbackEntry> toInvoke = null;
        Iterator<CallbackEntry> it = callbacks.iterator();
        while (it.hasNext()) {
            CallbackEntry entry = it.next();
            if (entry.done) {
                it.remove();
                continue;
            }
            if (version >= entry.targetVersion) {
                entry.done = true;
                entry.ex = ex;
                it.remove();
                if (entry.timeoutFuture != null) {
                    entry.timeoutFuture.cancel(false);
                }
                if (toInvoke == null) {
                    toInvoke = new LinkedList<>();
                }
                toInvoke.add(entry);
            }
        }
        return toInvoke;
    }

    private void invokeCallbacks(LinkedList<CallbackEntry> toInvoke) {
        if (toInvoke != null) {
            for (CallbackEntry entry : toInvoke) {
                invokeCallback(entry.callback, entry.ex);
            }
        }
    }

    private void invokeCallback(Consumer<Throwable> callback, Throwable ex) {
        try {
            callback.accept(ex);
        } catch (Throwable e) {
            log.error("callback failed", e);
        }
    }

    private static class CallbackEntry {
        Consumer<Throwable> callback;
        long targetVersion;
        ScheduledFuture<?> timeoutFuture;
        boolean done;
        Throwable ex;
    }
}
