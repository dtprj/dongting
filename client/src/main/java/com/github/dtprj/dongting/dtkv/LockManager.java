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
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author huangli
 */
class LockManager {
    private static final DtLog log = DtLogs.getLogger(LockManager.class);

    private final HashMap<Integer, HashMap<ByteArray, DtKvLockImpl>> lockMap = new HashMap<>();

    final KvClient kvClient;
    final RaftClient raftClient;
    final ScheduledExecutorService executeService;

    private int nextLockId = 1;

    LockManager(KvClient kvClient) {
        this.kvClient = kvClient;
        this.raftClient = kvClient.getRaftClient();
        this.executeService = raftClient.getNioClient().getBizExecutor() == null ?
                DtUtil.SCHEDULED_SERVICE : raftClient.getNioClient().getBizExecutor();
    }

    DtKvLockImpl createOrGetLock(int groupId, byte[] key) {
        Objects.requireNonNull(key);
        int c = KvClient.checkKey(key, KvClientConfig.MAX_KEY_SIZE, false, true);
        if (c != KvCodes.SUCCESS) {
            throw new IllegalArgumentException(KvCodes.toStr(c));
        }

        if (raftClient.getGroup(groupId) == null) {
            throw new RaftException("group not found: " + groupId);
        }

        ByteArray keyBytes = new ByteArray(key);
        synchronized (this) {
            HashMap<ByteArray, DtKvLockImpl> m = lockMap.get(groupId);
            if (m == null) {
                m = new HashMap<>();
                lockMap.put(groupId, m);
            }
            DtKvLockImpl dtKvLock = m.get(keyBytes);
            if (dtKvLock == null) {
                dtKvLock = new DtKvLockImpl(nextLockId++, this, groupId, key);
                m.put(keyBytes, dtKvLock);
            }
            return dtKvLock;
        }
    }

    void removeLock(int groupId, ByteArray key) {
        synchronized (this) {
            HashMap<ByteArray, DtKvLockImpl> m = lockMap.get(groupId);
            if (m != null) {
                m.remove(key);
                if (m.isEmpty()) {
                    lockMap.remove(groupId);
                }
            }
        }
    }

    void processLockPush(int groupId, KvReq req, int bizCode) {
        DtKvLockImpl lock;
        ByteArray key = new ByteArray(req.key);
        synchronized (this) {
            HashMap<ByteArray, DtKvLockImpl> groupLocks = lockMap.get(groupId);
            if (groupLocks == null) {
                log.info("no lock found for push: groupId={}, key={}", groupId, key);
                return;
            }
            lock = groupLocks.get(key);
            if (lock == null) {
                log.info("no lock found for push: groupId={}, key={}", groupId, key);
                return;
            }
        }
        lock.processLockPush(bizCode, req.value);
    }
}
