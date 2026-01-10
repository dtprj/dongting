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

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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

    /**
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
        long v;
        boolean needStart = false;
        lock.lock();
        try {
            v = ++requestVersion;
            if (!running) {
                running = true;
                needStart = true;
            }
        } finally {
            lock.unlock();
        }
        if (needStart) {
            executor.execute(this::runOnce);
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

        boolean ok;
        try {
            task.run();
            ok = true;
            failCount = 0;
        } catch (Throwable e) {
            ok = false;
            log.error("run task failed", e);
        }

        if (ok) {
            lock.lock();
            try {
                if (finishedVersion < currentTaskVersion) {
                    finishedVersion = currentTaskVersion;
                }
                doneCondition.signalAll();
                if (finishedVersion >= requestVersion) {
                    running = false;
                    return;
                }
            } finally {
                lock.unlock();
            }
            // still have pending requests, keep running in executor
            executor.execute(this::runOnce);
            return;
        }

        // failed
        if (retryIntervals == null) {
            // do not retry if no retry intervals configured
            lock.lock();
            try {
                // don't update finishedVersion since task failed
                // check if new requests arrived during task execution
                if (currentTaskVersion >= requestVersion) {
                    running = false;
                    return;
                }
            } finally {
                lock.unlock();
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
}
