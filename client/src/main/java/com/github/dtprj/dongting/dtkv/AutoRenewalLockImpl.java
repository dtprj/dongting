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
package com.github.dtprj.dongting.dtkv;

import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
class AutoRenewalLockImpl implements AutoRenewalLock {
    private static final DtLog log = DtLogs.getLogger(AutoRenewalLockImpl.class);

    // Retry intervals in milliseconds
    private final long[] retryIntervals;

    private final int groupId;
    private final ByteArray key;
    private final long leaseMillis;
    private final AutoRenewalLockListener listener;
    private final DistributedLockImpl lock;
    private final LockManager lockManager;

    private boolean locked;
    private boolean closed;

    private final LinkedList<Runnable> sequentialTasks = new LinkedList<>();
    private boolean running;

    private int retryIndex = 0;

    private int currentTaskId;
    private ScheduledFuture<?> scheduleTask;

    AutoRenewalLockImpl(int groupId, KvClient client, ByteArray key, long leaseMillis, AutoRenewalLockListener listener,
                        DistributedLockImpl lock) {
        if (leaseMillis <= 1) {
            throw new IllegalArgumentException("leaseMillis too small: " + leaseMillis);
        }
        DistributedLockImpl.checkLeaseMillis(leaseMillis);
        this.groupId = groupId;
        this.key = key;
        this.leaseMillis = leaseMillis;
        this.listener = listener;
        this.lock = lock;
        this.lockManager = client.lockManager;
        this.retryIntervals = client.config.autoRenewalRetryMillis;
        lock.expireListener = this::onExpire;
        scheduleTask(currentTaskId, 0);
    }

    private void sequentialRun(boolean markClose, boolean markExpire, Runnable nextTask) {
        Objects.requireNonNull(nextTask);
        boolean firstLoop = true;
        while (true) {
            Runnable r;
            synchronized (this) {
                if (firstLoop) {
                    if (markClose || markExpire) {
                        if (closed) {
                            return;
                        }
                        if (markClose) {
                            closed = true;
                            sequentialTasks.clear();
                        }
                        // run the task in lockManager.executeService to avoid user callback block
                        lockManager.submitTask(() -> sequentialRun(false, false, nextTask));
                        return;
                    } else if (running) {
                        sequentialTasks.addLast(nextTask);
                        return;
                    } else {
                        running = true;
                        firstLoop = false;
                        r = nextTask;
                    }
                } else {
                    r = sequentialTasks.pollFirst();
                }
                if (r == null) {
                    running = false;
                    return;
                }
            } // end synchronized block

            // run outside synchronized block
            try {
                r.run();
            } catch (Throwable e) {
                log.error("LinearQueue task error", e);
            }
        }
    }

    private void onExpire() {
        sequentialRun(false, true, () -> {
            if (closed) {
                return;
            }
            currentTaskId++; // cancel existing task serials
            cancelTask();
            doRunTask(currentTaskId, true);
        });
    }

    private void runTask(int taskId) {
        sequentialRun(false, false, () -> {
            if (taskId != currentTaskId) {
                return;
            }
            scheduleTask = null;
            doRunTask(taskId, true);
        });
    }

    private void doRunTask(int taskId, boolean updateLeaseImmediately) {
        long leaseRest = lock.getLeaseRestMillis();
        if (leaseRest > 1) {
            changeStateIfNeeded(true);
            if (updateLeaseImmediately) {
                try {
                    lock.updateLease(leaseMillis, (v, ex) -> rpcCallback(taskId, false, ex));
                } catch (Throwable e) {
                    handleRpcFail(taskId, "updateLease", e);
                }
            } else {
                scheduleTask(taskId, leaseRest / 2);
            }
        } else {
            changeStateIfNeeded(false);
            try {
                lock.tryLock(leaseMillis, leaseMillis, (result, ex) -> rpcCallback(taskId, true, ex));
            } catch (Throwable e) {
                handleRpcFail(taskId, "tryLock", e);
            }
        }
    }

    private void rpcCallback(int taskId, boolean tryLock, Throwable ex) {
        sequentialRun(false, false, () -> {
            if (taskId != currentTaskId) {
                return;
            }
            if (ex != null) {
                handleRpcFail(taskId, tryLock ? "tryLock" : "updateLease", ex);
            } else {
                retryIndex = 0;
                doRunTask(taskId, false);
            }
        });
    }

    private void changeStateIfNeeded(boolean newLocked) {
        try {
            if (locked != newLocked) {
                locked = newLocked;
                if (newLocked) {
                    listener.onAcquired();
                } else {
                    listener.onLost();
                }
            }
        } catch (Throwable e) {
            log.error("error in lock callback", e);
        }

    }

    private void handleRpcFail(int taskId, String op, Throwable ex) {
        long delayMillis;
        if (retryIndex < retryIntervals.length) {
            delayMillis = retryIntervals[retryIndex];
        } else {
            delayMillis = retryIntervals[retryIntervals.length - 1];
        }
        log.warn("{} failed, retry after {}ms. key={}, groupId={}, retryIndex={}",
                op, delayMillis, key, groupId, retryIndex, ex);
        retryIndex++;
        scheduleTask(taskId, delayMillis);
    }

    private void scheduleTask(int taskId, long delayMillis) {
        cancelTask();
        scheduleTask = lockManager.scheduleTask(() -> runTask(taskId), delayMillis, TimeUnit.MILLISECONDS);
    }

    private void cancelTask() {
        // Already in write lock
        ScheduledFuture<?> currentTask = scheduleTask;
        if (currentTask != null && !currentTask.isDone()) {
            currentTask.cancel(false);
        }
        scheduleTask = null;
    }

    @Override
    public boolean isHeldByCurrentClient() {
        return lock.isHeldByCurrentClient();
    }

    @Override
    public long getLeaseRestMillis() {
        return lock.getLeaseRestMillis();
    }

    @Override
    public void close() {
        lockManager.removeLock(lock);
    }

    public void closeImpl() {
        sequentialRun(true, false, () -> {
            currentTaskId++;  // cancel existing task serials
            cancelTask();
            lock.closeImpl();
            changeStateIfNeeded(false);
        });
    }
}
