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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
class AutoRenewalLockImpl implements AutoRenewalLock {
    private static final DtLog log = DtLogs.getLogger(AutoRenewalLockImpl.class);

    // Retry intervals in milliseconds
    private static final long[] RETRY_INTERVALS = {1000, 10_000, 30_000, 60_000};

    private final int groupId;
    private final ByteArray key;
    private final long leaseMillis;
    private final AutoRenewalLockListener listener;
    private final DistributedLockImpl lock;
    private final LockManager lockManager;

    private boolean locked;
    private boolean closed;
    private final ReentrantLock opLock = new ReentrantLock();

    private int retryIndex = 0;

    private int taskId;
    private ScheduledFuture<?> task;

    AutoRenewalLockImpl(int groupId, ByteArray key, long leaseMillis, AutoRenewalLockListener listener,
                        DistributedLockImpl lock, LockManager lockManager) {
        if (leaseMillis <= 1) {
            throw new IllegalArgumentException("leaseMillis too small: " + leaseMillis);
        }
        DistributedLockImpl.checkLeaseMillis(leaseMillis);
        this.groupId = groupId;
        this.key = key;
        this.leaseMillis = leaseMillis;
        this.listener = listener;
        this.lock = lock;
        this.lockManager = lockManager;
        lock.expireListener = this::onExpire;
        scheduleTaskInLock(0);
    }

    private void onExpire() {
        opLock.lock();
        try {
            if (closed) {
                return;
            }
            locked = false;
            cancelTaskInLock();
            try {
                listener.onLost();
            } catch (Throwable e) {
                log.error("Error in onLost listener", e);
            }
            doRunTask();
        } finally {
            opLock.unlock();
        }
    }

    private void runTask(int expiredTaskId) {
        opLock.lock();
        try {
            if (closed) {
                return;
            }
            if (expiredTaskId != taskId) {
                return;
            }
            doRunTask();
        } finally {
            opLock.unlock();
        }
    }

    private void doRunTask() {
        boolean tryLock = !locked;
        try {
            if (tryLock) {
                lock.tryLock(leaseMillis, leaseMillis, (result, ex) -> rpcCallback(tryLock, result, ex));
            } else {
                lock.updateLease(leaseMillis, (result, ex) -> rpcCallback(tryLock, null, ex));
            }
        } catch (Throwable e) {
            handleRpcFail(tryLock ? "tryLock" : "updateLease", e);
        }
    }

    private void rpcCallback(boolean tryLock, Boolean result, Throwable ex) {
        // DistributedLockImpl executes the callback without acquire DistributedLockImpl's opLock
        opLock.lock();
        try {
            if (closed) {
                return;
            }

            if (ex != null) {
                handleRpcFail(tryLock ? "tryLock" : "updateLease", ex);
            } else {
                retryIndex = 0; // Reset retry index on successful acquisition
                if (tryLock) {
                    if (result != null && result) {
                        // Successfully acquired the lock
                        locked = true;
                        try {
                            listener.onAcquired();
                        } catch (Throwable e) {
                            log.error("Error in onAcquired listener", e);
                        }
                        scheduleTaskInLock(leaseMillis / 2);
                    } else {
                        // Lock is held by others, retry immediately
                        doRunTask();
                    }
                } else {
                    scheduleTaskInLock(leaseMillis / 2);
                }
            }
        } finally {
            opLock.unlock();
        }
    }

    private void handleRpcFail(String op, Throwable ex) {
        log.warn("{} failed. key={}, groupId={}, retryIndex={}", op, key, groupId, retryIndex, ex);
        long delayMillis = calcDelayMillis();
        scheduleTaskInLock(delayMillis);
    }

    private long calcDelayMillis() {
        long delayMillis;
        if (retryIndex < RETRY_INTERVALS.length) {
            delayMillis = RETRY_INTERVALS[retryIndex];
            retryIndex++;
        } else {
            delayMillis = RETRY_INTERVALS[RETRY_INTERVALS.length - 1];
        }
        return delayMillis;
    }

    private void scheduleTaskInLock(long delayMillis) {
        // Cancel any existing task before scheduling new one
        taskId++;
        cancelTaskInLock();
        int expectTaskId = taskId;
        task = lockManager.executeService.schedule(() -> runTask(expectTaskId), delayMillis, TimeUnit.MILLISECONDS);
    }

    private void cancelTaskInLock() {
        // Already in write lock
        ScheduledFuture<?> currentTask = task;
        if (currentTask != null && !currentTask.isDone()) {
            currentTask.cancel(false);
        }
        task = null;
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
        opLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            cancelTaskInLock();
            lock.closeImpl();
            if (locked) {
                try {
                    listener.onLost();
                } catch (Throwable e) {
                    log.error("Error in onLost listener", e);
                }
            }
        } finally {
            opLock.unlock();
        }
    }
}
