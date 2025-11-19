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
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftClient;
import com.github.dtprj.dongting.raft.RaftException;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public class LockManager {
    private static final DtLog log = DtLogs.getLogger(LockManager.class);

    private final HashMap<Integer, HashMap<ByteArray, LockHolder>> lockMap = new HashMap<>();
    final ReentrantLock managerOpLock = new ReentrantLock();

    final KvClient kvClient;
    final RaftClient raftClient;
    private ExecutorService executeService;

    private static volatile ExecutorService fallbackExecutor;

    private int nextLockId = 1;

    protected LockManager(KvClient kvClient) {
        this.kvClient = kvClient;
        this.raftClient = kvClient.getRaftClient();
    }

    private static ExecutorService getFallbackExecutor() {
        ExecutorService es = fallbackExecutor;
        if (es == null) {
            synchronized (LockManager.class) {
                es = fallbackExecutor;
                if (es == null) {
                    es = Executors.newSingleThreadExecutor(r -> {
                        Thread t = new Thread(r, "DtKvClientFallbackExecutor");
                        t.setDaemon(true);
                        return t;
                    });
                    fallbackExecutor = es;
                }
            }
        }
        return es;
    }

    public void init(ExecutorService es) {
        if (es != null) {
            executeService = es;
        } else {
            executeService = getFallbackExecutor();
        }
    }

    private static class LockHolder {
        final DistributedLockImpl lock;
        final AutoRenewalLockImpl wrapper;

        LockHolder(DistributedLockImpl lock, AutoRenewalLockImpl wrapper) {
            this.lock = lock;
            this.wrapper = wrapper;
        }
    }

    DistributedLock createLock(int groupId, byte[] key, Runnable expireListener) {
        if (raftClient.getGroup(groupId) == null) {
            throw new RaftException("group not found: " + groupId);
        }

        ByteArray keyBytes = new ByteArray(key);
        managerOpLock.lock();
        try {
            HashMap<ByteArray, LockHolder> m = lockMap.computeIfAbsent(groupId, k -> new HashMap<>());
            LockHolder h = m.get(keyBytes);
            if (h == null) {
                DistributedLockImpl l = createLockImpl(groupId, nextLockId++, keyBytes, expireListener);
                h = new LockHolder(l, null);
                m.put(keyBytes, h);
                return h.lock;
            } else {
                throw new IllegalStateException("lock exists for the key: " + keyBytes);
            }
        } finally {
            managerOpLock.unlock();
        }
    }

    AutoRenewalLock createAutoRenewLock(int groupId, byte[] key, long leaseMillis, AutoRenewalLockListener listener) {
        if (raftClient.getGroup(groupId) == null) {
            throw new RaftException("group not found: " + groupId);
        }

        ByteArray keyBytes = new ByteArray(key);
        managerOpLock.lock();
        try {
            HashMap<ByteArray, LockHolder> m = lockMap.computeIfAbsent(groupId, k -> new HashMap<>());
            LockHolder h = m.get(keyBytes);
            if (h == null) {
                // the expireListener is set in AutoRenewLockImpl constructor
                DistributedLockImpl lock = createLockImpl(groupId, nextLockId++, keyBytes, null);
                AutoRenewalLockImpl wrapper = new AutoRenewalLockImpl(groupId, kvClient, keyBytes, leaseMillis, listener, lock);
                h = new LockHolder(lock, wrapper);
                m.put(keyBytes, h);
                return wrapper;
            } else {
                throw new IllegalStateException("lock exists for the key: " + keyBytes);
            }
        } finally {
            managerOpLock.unlock();
        }
    }

    // for unit test
    protected DistributedLockImpl createLockImpl(int groupId, int lockId, ByteArray key, Runnable expireListener) {
        return new DistributedLockImpl(lockId, this, groupId, key, expireListener);
    }

    // for unit test
    protected long getAutoRenewalMinValidLeaseMillis() {
        return 1;
    }

    void removeLock(DistributedLockImpl lock) {
        managerOpLock.lock();
        try {
            HashMap<ByteArray, LockHolder> m = lockMap.get(lock.groupId);
            if (m == null) {
                log.error("no lock map found, groupId={}, key={}", lock.groupId, lock.key);
                return;
            }
            LockHolder h = m.get(lock.key);
            if (h == null) {
                log.error("no lock found, groupId={}, key={}", lock.groupId, lock.key);
                return;
            }

            if (h.lock != lock) {
                log.error("lock not same, groupId={}, key={}", lock.groupId, lock.key);
                return;
            }

            if (h.wrapper != null) {
                h.wrapper.closeImpl();
            } else {
                h.lock.closeImpl();
            }

            m.remove(lock.key);
            if (m.isEmpty()) {
                lockMap.remove(lock.groupId);
            }
        } finally {
            managerOpLock.unlock();
        }
    }

    void removeAllLock() {
        managerOpLock.lock();
        try {
            for (HashMap<ByteArray, LockHolder> m : lockMap.values()) {
                for (LockHolder h : m.values()) {
                    if (h.wrapper != null) {
                        h.wrapper.closeImpl();
                    } else {
                        h.lock.closeImpl();
                    }
                }
            }
            lockMap.clear();
        } finally {
            managerOpLock.unlock();
        }
    }

    void processLockPush(int groupId, KvReq req, int bizCode) {
        LockHolder h;
        ByteArray key = new ByteArray(req.key);
        this.managerOpLock.lock();
        try {
            HashMap<ByteArray, LockHolder> groupLocks = lockMap.get(groupId);
            if (groupLocks == null) {
                log.info("group lock map not found: groupId={}, key={}", groupId, key);
                return;
            }
            h = groupLocks.get(key);
            if (h == null) {
                log.info("lock not found: groupId={}, key={}", groupId, key);
                return;
            }
        } finally {
            this.managerOpLock.unlock();
        }
        h.lock.processLockPush(bizCode, req.value, req.ttlMillis);
    }

    Future<?> scheduleTask(Runnable task, long delay, TimeUnit unit) {
        if (delay == 0) {
            return submitTask(task);
        } else {
            // run task in executeService, don't block DtUtil.SCHEDULED_SERVICE
            Runnable r = () -> submitTask(task);
            return DtUtil.SCHEDULED_SERVICE.schedule(r, delay, unit);
        }
    }

    Future<?> submitTask(Runnable task) {
        try {
            return executeService.submit(task);
        } catch (RejectedExecutionException e) {
            log.error("task submit rejected, run it in fallback executor", e);
            return getFallbackExecutor().submit(task);
        }
    }
}
