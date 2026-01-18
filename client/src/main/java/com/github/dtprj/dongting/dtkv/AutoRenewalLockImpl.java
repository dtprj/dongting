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

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
class AutoRenewalLockImpl extends AbstractLifeCircle implements AutoRenewalLock {
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
    private boolean rpcInProgress;

    private AutoRenewTask currentTask;

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
        this.currentTask = new AutoRenewTask();
    }

    @Override
    public void doStart() {
        this.currentTask.scheduleFuture = lockManager.submitTask(currentTask);
    }

    private boolean isClosed() {
        return status >= AbstractLifeCircle.STATUS_STOPPING;
    }

    private final class AutoRenewTask implements Runnable {
        private int retryIndex;
        private Future<?> scheduleFuture;
        private boolean thisTaskGetLock; // false -> true, and never change to false

        @Override
        public void run() {
            if (isClosed()) {
                return;
            }
            if (currentTask != this) {
                return;
            }
            scheduleFuture = null;
            long leaseRest = lock.getLeaseRestMillis();
            if (leaseRest > lockManager.getAutoRenewalMinValidLeaseMillis()) {
                sendRpc(false);
            } else {
                if (thisTaskGetLock) {
                    // Do nothing, break execute chain of this task. Wait for expire event to start a new task.
                    log.warn("lock lease is near to expire. key={}, restLeaseMillis={}", key, leaseRest);
                } else {
                    sendRpc(true);
                }
            }
        }

        private void sendRpc(boolean tryLock) {
            try {
                rpcInProgress = true;
                if (tryLock) {
                    lock.tryLock(leaseMillis, leaseMillis, (result, ex) -> rpcCallback(true, ex));
                } else {
                    lock.updateLease(leaseMillis, (v, ex) -> rpcCallback(false, ex));
                }
            } catch (Throwable e) {
                rpcCallback(tryLock, e);
            }
        }

        private void rpcCallback(boolean tryLock, Throwable ex) {
            rpcInProgress = false;
            if (isClosed()) {
                return;
            }
            if (currentTask != this) {
                if (currentTask != null) {
                    // ignore rpc callback action of "this" task, and run the new task
                    currentTask.run();
                }
                return;
            }
            String op = tryLock ? "tryLock" : "updateLease";
            if (ex != null) {
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
            } else {
                retryIndex = 0;
                if (lock.getLeaseRestMillis() > lockManager.getAutoRenewalMinValidLeaseMillis()) {
                    thisTaskGetLock = true;
                    changeStateIfNeeded(true);
                    schedule(leaseMillis / 2);
                } else {
                    run();
                }
            }
        }

        private void schedule(long delayMillis) {
            scheduleFuture = lockManager.scheduleTask(this, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    private void changeStateIfNeeded(boolean newLocked) {
        try {
            if (locked != newLocked) {
                locked = newLocked;
                if (newLocked) {
                    log.info("get lock: {}", lock.key);
                    listener.onAcquired(this);
                } else {
                    log.info("lost lock: {}", lock.key);
                    listener.onLost(this);
                }
            }
        } catch (Throwable e) {
            log.error("error in lock callback", e);
        }
    }

    private void onExpire() {
        if (isClosed()) {
            return;
        }
        if (!locked) {
            BugLog.getLog().error("lock is not locked");
            return;
        }
        cancelTask();
        changeStateIfNeeded(false);

        currentTask = new AutoRenewTask();
        //noinspection StatementWithEmptyBody
        if (rpcInProgress) {
            // If rpc is in progress, any rpc operation will be rejected in DistributedLockImpl,
            // so we run new task in rpcCallback method in the old task.
        } else {
            currentTask.run();
        }
    }

    private void cancelTask() {
        if (currentTask.scheduleFuture != null && !currentTask.scheduleFuture.isDone()) {
            currentTask.scheduleFuture.cancel(false);
        }
        currentTask = null;
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
    public void doStop(DtTime timeout, boolean force) {
        lockManager.removeLock(lock);
    }

    public void closeImpl() {
        // if we call KvClient.close(), it will close all locks and then close NioClient immediately,
        // the unlock operation in DistributedLockImpl.closeImpl() may fail if we call it in the callback
        // of fireCallbackTask, so we move it out of the callback.
        lock.closeImpl(); // the method is thread safe

        // use fireCallbackTask to ensure sequential execution with other callbacks
        lock.fireCallbackTask(() -> {
            cancelTask();
            changeStateIfNeeded(false);
        });
    }
}
