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

import java.util.concurrent.Future;
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

    // DistributedLockImpl ensures sequential run of callbacks (include onExpire), and the invocation of
    // a callback is happened-before the next one, so no extra synchronization is needed except for closeImpl() method.
    private boolean locked;
    private boolean closed;

    private int retryIndex = 0;

    private int currentTaskId;
    private Future<?> scheduleTask;

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
        schedule(0);
    }

    private void onExpire() {
        if (closed) {
            return;
        }
        cancelTask();
        doRunTask(true);
    }

    private void schedule(long delayMillis) {
        cancelTask();
        int taskId = ++currentTaskId;
        scheduleTask = lockManager.scheduleTask(() -> {
            if (taskId != currentTaskId) {
                return;
            }
            scheduleTask = null;
            doRunTask(true);
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    private void doRunTask(boolean updateLeaseImmediately) {
        long leaseRest = lock.getLeaseRestMillis();
        int taskId = currentTaskId;
        if (leaseRest > 1) {
            changeStateIfNeeded(true);
            if (updateLeaseImmediately) {
                try {
                    lock.updateLease(leaseMillis, (v, ex) -> rpcCallback(taskId, false, ex));
                } catch (Throwable e) {
                    handleRpcFail(taskId, "updateLease", e);
                }
            } else {
                schedule(leaseRest / 2);
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
        if (taskId != currentTaskId) {
            return;
        }
        if (ex != null) {
            handleRpcFail(taskId, tryLock ? "tryLock" : "updateLease", ex);
        } else {
            retryIndex = 0;
            doRunTask(false);
        }
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
        if (taskId != currentTaskId) {
            return;
        }
        long delayMillis;
        if (retryIndex < retryIntervals.length) {
            delayMillis = retryIntervals[retryIndex];
        } else {
            delayMillis = retryIntervals[retryIntervals.length - 1];
        }
        log.warn("{} failed, retry after {}ms. key={}, groupId={}, retryIndex={}",
                op, delayMillis, key, groupId, retryIndex, ex);
        retryIndex++;
        schedule(delayMillis);
    }

    private void cancelTask() {
        Future<?> currentTask = scheduleTask;
        if (currentTask != null && !currentTask.isDone()) {
            currentTaskId++;
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
        // use fireCallbackTask to ensure sequential execution with other callbacks
        lock.fireCallbackTask(() -> {
            if (closed) {
                return;
            }
            closed = true;
            cancelTask();
            lock.closeImpl();
            changeStateIfNeeded(false);
        });
    }
}
