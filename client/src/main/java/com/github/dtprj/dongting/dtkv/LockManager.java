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
import com.github.dtprj.dongting.raft.RaftClient;
import com.github.dtprj.dongting.raft.RaftException;

import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
class LockManager {
    private static final DtLog log = DtLogs.getLogger(LockManager.class);

    private final HashMap<Integer, HashMap<ByteArray, LockHolder>> lockMap = new HashMap<>();
    final ReentrantLock managerOpLock = new ReentrantLock();

    final KvClient kvClient;
    final RaftClient raftClient;
    ScheduledExecutorService executeService;

    private int nextLockId = 1;

    LockManager(KvClient kvClient) {
        this.kvClient = kvClient;
        this.raftClient = kvClient.getRaftClient();
    }

    private static class LockHolder {
        final DistributedLockImpl lock;
        final AutoRenewLock wrapper;

        LockHolder(DistributedLockImpl lock, AutoRenewLock wrapper) {
            this.lock = lock;
            this.wrapper = wrapper;
        }
    }

    DistributedLock createLock(int groupId, byte[] key) {
        if (raftClient.getGroup(groupId) == null) {
            throw new RaftException("group not found: " + groupId);
        }

        ByteArray keyBytes = new ByteArray(key);
        managerOpLock.lock();
        try {
            HashMap<ByteArray, LockHolder> m = lockMap.computeIfAbsent(groupId, k -> new HashMap<>());
            LockHolder h = m.get(keyBytes);
            if (h == null) {
                h = new LockHolder(new DistributedLockImpl(nextLockId++, this, groupId, keyBytes), null);
                m.put(keyBytes, h);
                return h.lock;
            } else {
                throw new IllegalStateException("lock exists for the key: " + keyBytes);
            }
        } finally {
            managerOpLock.unlock();
        }
    }

    AutoRenewLock createAutoRenewLock(int groupId, byte[] key, long leaseMillis, AutoRenewLockListener listener) {
        if (raftClient.getGroup(groupId) == null) {
            throw new RaftException("group not found: " + groupId);
        }

        ByteArray keyBytes = new ByteArray(key);
        managerOpLock.lock();
        try {
            HashMap<ByteArray, LockHolder> m = lockMap.computeIfAbsent(groupId, k -> new HashMap<>());
            LockHolder h = m.get(keyBytes);
            if (h == null) {
                DistributedLockImpl lock = new DistributedLockImpl(nextLockId++, this, groupId, keyBytes);
                AutoRenewLock wrapper = new AutoRenewLockImpl(groupId, keyBytes, leaseMillis, listener, lock, executeService);
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

    void removeLock(DistributedLockImpl lock) {
        managerOpLock.lock();
        try {
            lock.closeImpl();
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
                    h.lock.closeImpl();
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
                log.info("no h found for push: groupId={}, key={}", groupId, key);
                return;
            }
            h = groupLocks.get(key);
            if (h == null) {
                log.info("no h found for push: groupId={}, key={}", groupId, key);
                return;
            }
        } finally {
            this.managerOpLock.unlock();
        }
        h.lock.processLockPush(bizCode, req.value);
    }
}
