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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvClientConfig;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.sm.Snapshot;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

/**
 * All write operations run in same thread, and there are multiple read threads, so the write thread
 * do not need to acquire StampedLock if it only read data or update fields that read threads will not access.
 *
 * @author huangli
 */
class KvImpl {
    private static final DtLog log = DtLogs.getLogger(KvImpl.class);

    private static final int GC_ITEMS = 500;

    // only update int unit test
    int maxKeySize = KvClientConfig.MAX_KEY_SIZE;
    int maxValueSize = KvClientConfig.MAX_VALUE_SIZE;
    int gcItems = GC_ITEMS;

    private final int groupId;

    // When iterating over this map, we need to divide the process into multiple steps,
    // with each step only accessing a portion of the map. Therefore, ConcurrentHashMap is needed here.
    final ConcurrentHashMap<ByteArray, KvNodeHolder> map;


    // for fast access root dir
    final KvNodeHolder root;

    // write operations is not atomic, so we need lock although ConcurrentHashMap is used
    private final StampedLock lock = new StampedLock();

    private final Timestamp ts;

    private final ArrayList<Snapshot> openSnapshots = new ArrayList<>();
    private long maxOpenSnapshotIndex = 0;
    private long minOpenSnapshotIndex = 0;

    private final ServerWatchManager watchManager;
    private final IndexedQueue<KvNodeHolder> updateQueue = new IndexedQueue<>(32);

    private final TtlManager ttlManager;

    public KvImpl(ServerWatchManager watchManager, TtlManager ttlManager, Timestamp ts, int groupId,
                  int initCapacity, float loadFactor) {
        this.watchManager = watchManager;
        this.ts = ts;
        this.groupId = groupId;
        this.map = new ConcurrentHashMap<>(initCapacity, loadFactor);
        KvNodeEx n = new KvNodeEx(0, 0, 0, 0,
                KvNode.FLAG_DIR_MASK, null);
        this.root = new KvNodeHolder(ByteArray.EMPTY, ByteArray.EMPTY, n, null);
        this.map.put(ByteArray.EMPTY, root);
        this.ttlManager = ttlManager;
    }

    static KvResult checkExistNode(KvNodeHolder h, KvImpl.OpContext ctx) {
        KvNodeEx latest = h == null ? null : h.latest;
        switch (ctx.bizType) {
            case DtKV.BIZ_TYPE_PUT:
            case DtKV.BIZ_TYPE_MKDIR:
            case DtKV.BIZ_TYPE_BATCH_PUT:
            case DtKV.BIZ_TYPE_CAS:
                if (h == null || latest.removed) {
                    return null;
                }
                if ((latest.flag & KvNode.FLAG_LOCK_MASK) != 0) {
                    return new KvResult(KvCodes.IS_LOCK_NODE);
                }
                if (latest.ttlInfo != null) {
                    return new KvResult(KvCodes.IS_TEMP_NODE);
                }
                return null;
            case DtKV.BIZ_TYPE_REMOVE:
            case DtKV.BIZ_TYPE_BATCH_REMOVE:
                if (h == null || latest.removed) {
                    return KvResult.NOT_FOUND;
                }
                if ((latest.flag & KvNode.FLAG_LOCK_MASK) != 0) {
                    return new KvResult(KvCodes.IS_LOCK_NODE);
                }
                if (latest.ttlInfo != null && !latest.ttlInfo.owner.equals(ctx.operator)) {
                    return new KvResult(KvCodes.NOT_OWNER);
                }
                return null;
            case DtKV.BIZ_TYPE_UPDATE_TTL:
                if (h == null || latest.removed) {
                    return KvResult.NOT_FOUND;
                }
                if (latest.ttlInfo == null) {
                    return new KvResult(KvCodes.NOT_TEMP_NODE);
                }
                if (!latest.ttlInfo.owner.equals(ctx.operator)) {
                    return new KvResult(KvCodes.NOT_OWNER);
                }
                return null;
            case DtKV.BIZ_MK_TEMP_DIR:
            case DtKV.BIZ_TYPE_PUT_TEMP_NODE:
                if (h == null || latest.removed) {
                    return null;
                }
                if ((latest.flag & KvNode.FLAG_LOCK_MASK) != 0) {
                    return new KvResult(KvCodes.IS_LOCK_NODE);
                }
                if (latest.ttlInfo == null) {
                    return new KvResult(KvCodes.NOT_TEMP_NODE);
                }
                if (!latest.ttlInfo.owner.equals(ctx.operator)) {
                    return new KvResult(KvCodes.NOT_OWNER);
                }
                return null;
            case DtKV.BIZ_TYPE_TRY_LOCK:
            case DtKV.BIZ_TYPE_UNLOCK:
            case DtKV.BIZ_TYPE_UPDATE_LOCK_LEASE:
                if (h == null || latest.removed) {
                    return ctx.bizType == DtKV.BIZ_TYPE_TRY_LOCK ? null : KvResult.NOT_FOUND;
                }
                if ((latest.flag & KvNode.FLAG_LOCK_MASK) == 0 || (latest.flag & KvNode.FLAG_DIR_MASK) == 0) {
                    return new KvResult(KvCodes.NOT_LOCK_NODE);
                }
                return null;
            case DtKV.BIZ_TYPE_EXPIRE:
                // call by raft leader, do not call this method
            default:
                throw new IllegalStateException(String.valueOf(ctx.bizType));
        }
    }

    static KvResult checkParentBeforePut(KvNodeHolder parent, KvImpl.OpContext ctx) {
        if (parent == null || parent.latest.removed) {
            return new KvResult(KvCodes.PARENT_DIR_NOT_EXISTS);
        }
        if ((parent.latest.flag & KvNode.FLAG_DIR_MASK) == 0) {
            return new KvResult(KvCodes.PARENT_NOT_DIR);
        }
        if ((parent.latest.flag & KvNode.FLAG_LOCK_MASK) != 0) {
            return new KvResult(KvCodes.PARENT_IS_LOCK);
        }
        switch (ctx.bizType) {
            case DtKV.BIZ_TYPE_TRY_LOCK:
                while (parent != null) {
                    if (parent.latest.ttlInfo != null) {
                        return new KvResult(KvCodes.UNDER_TEMP_DIR);
                    }
                    parent = parent.parent;
                }
                return null;
            case DtKV.BIZ_TYPE_PUT:
            case DtKV.BIZ_TYPE_MKDIR:
            case DtKV.BIZ_TYPE_BATCH_PUT:
            case DtKV.BIZ_TYPE_CAS:
            case DtKV.BIZ_TYPE_PUT_TEMP_NODE:
            case DtKV.BIZ_MK_TEMP_DIR:
                return null;
            default:
                throw new IllegalStateException(String.valueOf(ctx.bizType));
        }
    }

    private void addToUpdateQueue(long updateIndex, KvNodeHolder h) {
        h.updateIndex = updateIndex;
        if (h.watchHolder == null || h.watchHolder.watches.isEmpty()) {
            return;
        }
        if (h.inUpdateQueue) {
            return;
        }
        h.inUpdateQueue = true;
        updateQueue.addLast(h);
    }

    private void afterUpdate() {
        if (watchManager == null) {
            return;
        }
        try {
            IndexedQueue<KvNodeHolder> q = updateQueue;
            for (int s = q.size(), i = 0; i < s; i++) {
                KvNodeHolder h = q.removeFirst();
                h.inUpdateQueue = false;
                watchManager.afterUpdate(h);
            }
        } catch (Exception e) {
            BugLog.log(e);
        }
    }

    int checkKey(ByteArray key, boolean allowEmpty, boolean fullCheck) {
        if (key != null && key.isSlice()) {
            // slice key is not allowed
            return KvCodes.INVALID_KEY;
        }
        byte[] bs = key == null ? null : key.getData();
        return KvClient.checkKey(bs, maxKeySize, allowEmpty, fullCheck);
    }

    /**
     * This method may be called in other threads.
     * <p>
     * For simplification, this method reads the latest snapshot, rather than the one specified by
     * the raftIndex parameter, and this does not violate linearizability.
     */
    public KvResult get(ByteArray key) {
        int ck = checkKey(key, true, false);
        if (ck != KvCodes.SUCCESS) {
            return new KvResult(ck);
        }
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            KvResult r = get0(key);
            if (lock.validate(stamp)) {
                return r;
            }
        }
        stamp = lock.readLock();
        try {
            return get0(key);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    private KvResult get0(ByteArray key) {
        KvNodeHolder h;
        if (key == null || key.length == 0) {
            h = root;
        } else {
            h = map.get(key);
        }
        if (h == null) {
            return KvResult.NOT_FOUND;
        }
        KvNodeEx kvNode = h.latest;
        if (kvNode.removed) {
            return KvResult.NOT_FOUND;
        }
        return new KvResult(KvCodes.SUCCESS, kvNode, null);
    }

    /**
     * This method may be called in other threads.
     * <p>
     * For simplification, this method reads the latest snapshot, rather than the one specified by
     * the raftIndex parameter, and this does not violate linearizability.
     */
    public Pair<Integer, List<KvResult>> batchGet(List<byte[]> keys) {
        if (keys == null || keys.isEmpty()) {
            return new Pair<>(KvCodes.INVALID_KEY, null);
        }
        int s = keys.size();
        ArrayList<KvResult> list = new ArrayList<>(s);
        long stamp = lock.readLock();
        try {
            for (int i = 0; i < s; i++) {
                byte[] bs = keys.get(i);
                ByteArray key = bs == null ? null : new ByteArray(bs);
                int ck = checkKey(key, false, false);
                if (ck != KvCodes.SUCCESS) {
                    list.add(new KvResult(ck));
                } else {
                    list.add(get0(key));
                }
            }
        } finally {
            lock.unlockRead(stamp);
        }
        return new Pair<>(KvCodes.SUCCESS, list);
    }

    /**
     * This method may be called in other threads.
     * <p>
     * For simplification, this method reads the latest snapshot, rather than the one specified by
     * the raftIndex parameter, and this does not violate linearizability.
     */
    public Pair<Integer, List<KvResult>> list(ByteArray key) {
        int ck = checkKey(key, true, false);
        if (ck != KvCodes.SUCCESS) {
            return new Pair<>(ck, null);
        }
        long stamp = lock.readLock();
        try {
            KvNodeHolder h;
            if (key == null || key.getData().length == 0) {
                h = root;
            } else {
                h = map.get(key);
            }
            if (h == null) {
                return new Pair<>(KvCodes.NOT_FOUND, null);
            }
            KvNodeEx kvNode = h.latest;
            if (kvNode.removed) {
                return new Pair<>(KvCodes.NOT_FOUND, null);
            }
            if ((kvNode.flag & KvNode.FLAG_DIR_MASK) == 0) {
                return new Pair<>(KvCodes.PARENT_NOT_DIR, null);
            }
            ArrayList<KvResult> list = kvNode.list();
            return new Pair<>(KvCodes.SUCCESS, list);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    static final class OpContext {
        UUID operator;
        long ttlMillis;
        long leaderCreateTimeMillis;
        long localCreateNanos;
        int bizType;

        OpContext() {
        }

        public void init(int bizType, UUID operator, long ttlMillis, long leaderCreateTimeMillis, long localCreateNanos) {
            this.operator = operator;
            this.ttlMillis = ttlMillis;
            this.leaderCreateTimeMillis = leaderCreateTimeMillis;
            this.localCreateNanos = localCreateNanos;
            this.bizType = bizType;
        }
    }

    static final class KvResultWithNewOwnerInfo extends KvResult {
        final KvNodeEx newOwner;
        final long newOwnerServerSideWaitNanos;

        KvResultWithNewOwnerInfo(int bizCode, KvNodeEx newOwner, long newOwnerServerSideWaitNanos) {
            super(bizCode);
            this.newOwner = newOwner;
            this.newOwnerServerSideWaitNanos = newOwnerServerSideWaitNanos;
        }
    }

    public final OpContext opContext = new OpContext();

    public KvResult put(long index, ByteArray key, byte[] data) {
        if (data == null || data.length == 0) {
            return new KvResult(KvCodes.INVALID_VALUE);
        }
        if (data.length > maxValueSize) {
            return new KvResult(KvCodes.VALUE_TOO_LONG);
        }
        return checkAndPut(index, key, data, true);
    }

    private KvResult checkAndPut(long index, ByteArray key, byte[] data, boolean lock) {
        int ck = checkKey(key, false, false);
        if (ck != KvCodes.SUCCESS) {
            return new KvResult(ck);
        }
        KvNodeHolder parent;
        int lastIndexOfSep = key.lastIndexOf(KvClientConfig.SEPARATOR);
        if (lastIndexOfSep > 0) {
            ByteArray dirKey = key.sub(0, lastIndexOfSep);
            parent = map.get(dirKey);
            KvResult r = checkParentBeforePut(parent, opContext);
            if (r != null) {
                return r;
            }
        } else {
            parent = root;
        }
        KvNodeHolder h = map.get(key);
        KvResult r = checkExistNode(h, opContext);
        if (r != null) {
            return r;
        }
        long stamp = lock ? this.lock.writeLock() : 0;
        try {
            return doPutInLock(index, key, data, h, parent, lastIndexOfSep);
        } finally {
            if (lock) {
                this.lock.unlockWrite(stamp);
                afterUpdate();
            }
        }
    }

    private KvNodeHolder lastPutNodeHolder = null;

    private KvResult doPutInLock(long index, ByteArray key, byte[] data, KvNodeHolder current,
                                 KvNodeHolder parent, int lastIndexOfSep) {
        if (current != null && current.parent != parent) {
            BugLog.logAndThrow("current.parent != parent");
        }
        lastPutNodeHolder = null;
        if (current == null || current.latest.removed) {
            int flag = (data == null || data.length == 0 ? KvNode.FLAG_DIR_MASK : 0) |
                    (opContext.bizType == DtKV.BIZ_TYPE_TRY_LOCK ? KvNode.FLAG_LOCK_MASK : 0);
            KvNodeEx newKvNode = new KvNodeEx(index, opContext.leaderCreateTimeMillis, index,
                    opContext.leaderCreateTimeMillis, flag, data);
            if (current == null) {
                current = new KvNodeHolder(key, key.sub(lastIndexOfSep + 1), newKvNode, parent);
                map.put(key, current);
                parent.childHolderCount++;
                parent.latest.addChild(current);
            } else {
                updateHolderAndGc(current, newKvNode, current.latest);
            }
            KvResult r = KvResult.SUCCESS;
            if (opContext.bizType == DtKV.BIZ_TYPE_TRY_LOCK && (flag & KvNode.FLAG_DIR_MASK) == 0) {
                if (parent.latest.peekNextOwner() == current) {
                    // get the lock
                    opContext.ttlMillis = readHoldTtlMillis(data);
                } else {
                    r = new KvResult(KvCodes.LOCK_BY_OTHER);
                }
            }
            ttlManager.initTtl(index, current.key, newKvNode, opContext);
            if (watchManager != null) {
                watchManager.mountWatchToChild(current);
            }
            addToUpdateQueue(index, current);
            updateParent(index, opContext.leaderCreateTimeMillis, parent);
            lastPutNodeHolder = current;
            return r;
        } else {
            // has existing node
            KvNodeEx oldNode = current.latest;
            if (data == null || data.length == 0) {
                // new node is dir
                if ((oldNode.flag & KvNode.FLAG_DIR_MASK) == 0) {
                    // node type not match, return error response
                    return new KvResult(KvCodes.VALUE_EXISTS);
                } else {
                    // dir has already existed, do nothing
                    if (opContext.bizType == DtKV.BIZ_MK_TEMP_DIR) {
                        ttlManager.updateTtl(index, current.key, oldNode, opContext);
                    }
                    lastPutNodeHolder = current;
                    return new KvResult(KvCodes.DIR_EXISTS);
                }
            } else {
                // new node is not dir
                if ((oldNode.flag & KvNode.FLAG_DIR_MASK) != 0) {
                    // node type not match, return error response
                    return new KvResult(KvCodes.DIR_EXISTS);
                } else {
                    // update value
                    KvNodeEx newKvNode = new KvNodeEx(oldNode, index, opContext.leaderCreateTimeMillis, data);
                    updateHolderAndGc(current, newKvNode, oldNode);
                    KvResult r = KvResult.SUCCESS_OVERWRITE;
                    if (opContext.bizType == DtKV.BIZ_TYPE_PUT_TEMP_NODE || opContext.bizType == DtKV.BIZ_TYPE_TRY_LOCK) {
                        if (opContext.bizType == DtKV.BIZ_TYPE_TRY_LOCK) {
                            if (parent.latest.peekNextOwner() == current) {
                                // get the lock (already hold before)
                                opContext.ttlMillis = readHoldTtlMillis(data);
                                r = new KvResult(KvCodes.LOCK_BY_SELF);
                            } else {
                                r = new KvResult(KvCodes.LOCK_BY_OTHER);
                            }
                        }
                        ttlManager.updateTtl(index, current.key, newKvNode, opContext);
                    }
                    addToUpdateQueue(index, current);
                    updateParent(index, opContext.leaderCreateTimeMillis, parent);
                    lastPutNodeHolder = current;
                    return r;
                }
            }
        }
    }

    private void updateHolderAndGc(KvNodeHolder current, KvNodeEx newKvNode, KvNodeEx oldNode) {
        if (maxOpenSnapshotIndex > 0) {
            newKvNode.previous = oldNode;
            current.latest = newKvNode;
            gc(current);
        } else {
            current.latest = newKvNode;
        }
    }

    public Pair<Integer, List<KvResult>> batchPut(long index, List<byte[]> keys, List<byte[]> values) {
        if (keys == null || keys.isEmpty()) {
            return new Pair<>(KvCodes.INVALID_KEY, null);
        }
        int size = keys.size();
        ArrayList<KvResult> list = new ArrayList<>(size);
        if (values == null || values.size() != size) {
            return new Pair<>(KvCodes.INVALID_VALUE, null);
        }
        long stamp = lock.writeLock();
        try {
            for (int i = 0; i < size; i++) {
                byte[] k = keys.get(i);
                list.add(checkAndPut(index, k == null ? null : new ByteArray(k), values.get(i), false));
            }
        } finally {
            lock.unlockWrite(stamp);
            afterUpdate();
        }
        return new Pair<>(KvCodes.SUCCESS, list);
    }

    private void updateParent(long index, long timestamp, KvNodeHolder parent) {
        while (parent != null) {
            addToUpdateQueue(index, parent);
            KvNodeEx oldDirNode = parent.latest;
            parent.latest = new KvNodeEx(oldDirNode, index, timestamp, oldDirNode.data);
            if (maxOpenSnapshotIndex > 0) {
                parent.latest.previous = oldDirNode;
                gc(parent);
            }
            parent = parent.parent;
        }
    }

    private void gc(KvNodeHolder h) {
        KvNodeEx n = h.latest;
        if (maxOpenSnapshotIndex > 0) {
            KvNodeEx next = null;
            while (n != null) {
                if (next != null && n.updateIndex > maxOpenSnapshotIndex) {
                    next.previous = n.previous;
                } else if (next != null && next.updateIndex <= minOpenSnapshotIndex) {
                    next.previous = null;
                    return;
                } else if (n.removed) {
                    KvNodeEx p;
                    while ((p = n.previous) != null && (p.updateIndex > maxOpenSnapshotIndex
                            || n.updateIndex <= minOpenSnapshotIndex)) {
                        n.previous = p.previous;
                    }
                    if (p == null) {
                        if (next == null) {
                            tryRemoveFromMap(h);
                        } else {
                            next.previous = null;
                        }
                        return;
                    } else {
                        next = n;
                    }
                } else {
                    next = n;
                }
                n = n.previous;
            }
        } else {
            if (n.removed) {
                tryRemoveFromMap(h);
            } else {
                n.previous = null;
            }
        }
    }

    private void tryRemoveFromMap(KvNodeHolder h) {
        if (h.childHolderCount == 0) {
            map.remove(h.key);
            int c = --h.parent.childHolderCount;
            if (c < 0) {
                BugLog.logAndThrow("childHolderCount < 0");
            } else if (c == 0) {
                gc(h.parent);
            }
        }
    }

    void installSnapshotPut(EncodeStatus encodeStatus) {
        // do not need lock, no other requests during install snapshot
        KvNodeEx n = new KvNodeEx(encodeStatus.createIndex, encodeStatus.createTime, encodeStatus.updateIndex,
                encodeStatus.updateTime, encodeStatus.flag, encodeStatus.valueBytes);
        if (encodeStatus.keyBytes == null || encodeStatus.keyBytes.length == 0) {
            root.latest = n;
        } else {
            KvNodeHolder parent;
            ByteArray key = new ByteArray(encodeStatus.keyBytes);
            ByteArray keyInDir;
            int lastIndexOfSep = key.lastIndexOf(KvClientConfig.SEPARATOR);
            if (lastIndexOfSep == -1) {
                parent = root;
                keyInDir = key;
            } else {
                ByteArray dirKey = key.sub(0, lastIndexOfSep);
                parent = map.get(dirKey);
                keyInDir = key.sub(lastIndexOfSep + 1);
            }
            KvNodeHolder h = new KvNodeHolder(key, keyInDir, n, parent);
            map.put(key, h);
            parent.childHolderCount++;
            parent.latest.addChild(h);
            if (encodeStatus.ttlMillis > 0) {
                // nanos can't persist, use wallClockMillis, so has week dependence on system clock.
                long costTimeMillis = ts.wallClockMillis - encodeStatus.leaderTtlStartTime;
                if (costTimeMillis < 0) {
                    costTimeMillis = 0;
                }
                long localCreateNanos = ts.nanoTime - costTimeMillis * 1_000_000L;
                opContext.init(DtKV.BIZ_TYPE_PUT, new UUID(encodeStatus.uuid1, encodeStatus.uuid2),
                        encodeStatus.ttlMillis, encodeStatus.leaderTtlStartTime, localCreateNanos);
                ttlManager.initTtl(encodeStatus.ttlRaftIndex, key, n, opContext);
            }
        }
    }

    public Supplier<Boolean> createGcTask() {
        Iterator<KvNodeHolder> it = map.values().iterator();
        long t = System.currentTimeMillis();
        log.info("group {} start gc task", groupId);
        return () -> {
            long stamp = lock.writeLock();
            try {
                for (int i = 0; i < gcItems; i++) {
                    if (!it.hasNext()) {
                        log.info("group {} gc task finished, cost {} ms", groupId, System.currentTimeMillis() - t);
                        return Boolean.FALSE;
                    }
                    KvNodeHolder h = it.next();
                    gc(h);
                }
                return Boolean.TRUE;
            } finally {
                lock.unlockWrite(stamp);
            }
        };
    }

    public KvResult remove(long index, ByteArray key) {
        return checkAndRemove(index, key, true);
    }

    private KvResult checkAndRemove(long index, ByteArray key, boolean lock) {
        int ck = checkKey(key, false, false);
        if (ck != KvCodes.SUCCESS) {
            return new KvResult(ck);
        }
        KvNodeHolder h = map.get(key);
        KvResult r = checkExistNode(h, opContext);
        if (r != null) {
            return r;
        }
        KvNodeEx n = h.latest;
        if (n.childCount() > 0) {
            return new KvResult(KvCodes.HAS_CHILDREN);
        }
        long stamp = lock ? this.lock.writeLock() : 0;
        try {
            return doRemoveInLock(index, h);
        } finally {
            if (lock) {
                this.lock.unlockWrite(stamp);
                afterUpdate();
            }
        }
    }

    private KvResult doRemoveInLock(long index, KvNodeHolder h) {
        if (h.latest.removed) {
            BugLog.logAndThrow("already removed");
        }
        if (h.parent.latest.removed) {
            BugLog.logAndThrow("parent removed");
        }
        long logTime = opContext.leaderCreateTimeMillis;
        addToUpdateQueue(index, h);

        // The children list only used in list and remove check, and always read the latest data.
        // So we can remove it from children list safely even if there is a snapshot being reading.
        h.parent.latest.removeChild(h.keyInDir);

        ttlManager.remove(h.latest);

        if (maxOpenSnapshotIndex > 0) {
            KvNodeEx n = h.latest;
            KvNodeEx newKvNode = new KvNodeEx(n.createIndex, n.createTime, index, logTime);
            h.latest = newKvNode;
            newKvNode.previous = n;
            gc(h);
        } else {
            if (h.childHolderCount == 0) {
                map.remove(h.key);
                if (--h.parent.childHolderCount < 0) {
                    BugLog.logAndThrow("childHolderCount < 0");
                }
            } else {
                h.latest = new KvNodeEx(index, logTime, index, logTime);
                // no previous
            }
        }

        if (watchManager != null) {
            watchManager.mountWatchToParent(h);
        }

        updateParent(index, logTime, h.parent);
        return KvResult.SUCCESS;
    }

    public Pair<Integer, List<KvResult>> batchRemove(long index, List<byte[]> keys) {
        if (keys == null || keys.isEmpty()) {
            return new Pair<>(KvCodes.INVALID_KEY, null);
        }
        int size = keys.size();
        ArrayList<KvResult> list = new ArrayList<>(size);
        long stamp = lock.writeLock();
        try {
            for (int i = 0; i < size; i++) {
                byte[] k = keys.get(i);
                list.add(checkAndRemove(index, k == null ? null : new ByteArray(k), false));
            }
        } finally {
            lock.unlockWrite(stamp);
            afterUpdate();
        }
        return new Pair<>(KvCodes.SUCCESS, list);
    }

    public KvResult compareAndSet(long index, ByteArray key, byte[] expectedValue, byte[] newValue) {
        int ck = checkKey(key, false, false);
        if (ck != KvCodes.SUCCESS) {
            return new KvResult(ck);
        }
        if (expectedValue == null || expectedValue.length == 0) {
            if (newValue == null || newValue.length == 0) {
                // don't mkdir
                return new KvResult(KvCodes.INVALID_VALUE);
            }
        }
        KvNodeHolder parent;
        int lastIndexOfSep = key.lastIndexOf(KvClientConfig.SEPARATOR);
        if (lastIndexOfSep > 0) {
            ByteArray dirKey = key.sub(0, lastIndexOfSep);
            parent = map.get(dirKey);
            KvResult r = checkParentBeforePut(parent, opContext);
            if (r != null) {
                return r;
            }
        } else {
            parent = root;
        }
        KvNodeHolder h = map.get(key);
        KvResult r = checkExistNode(h, opContext);
        if (r != null) {
            return r;
        }
        long stamp = lock.writeLock();
        try {
            if (expectedValue == null || expectedValue.length == 0) {
                if (h == null || h.latest.removed) {
                    return doPutInLock(index, key, newValue, h, parent, lastIndexOfSep);
                } else {
                    return new KvResult(KvCodes.CAS_MISMATCH);
                }
            } else {
                if (h == null) {
                    return new KvResult(KvCodes.CAS_MISMATCH);
                }
                KvNodeEx n = h.latest;
                if (n.removed || (n.flag & KvNode.FLAG_DIR_MASK) != 0) {
                    return new KvResult(KvCodes.CAS_MISMATCH);
                }
                byte[] bs = n.data;
                if (bs == null || bs.length != expectedValue.length) {
                    return new KvResult(KvCodes.CAS_MISMATCH);
                }
                for (int i = 0; i < bs.length; i++) {
                    if (bs[i] != expectedValue[i]) {
                        return new KvResult(KvCodes.CAS_MISMATCH);
                    }
                }
                if (newValue == null || newValue.length == 0) {
                    return doRemoveInLock(index, h);
                } else {
                    r = doPutInLock(index, key, newValue, h, parent, lastIndexOfSep);
                    return r == KvResult.SUCCESS_OVERWRITE ? KvResult.SUCCESS : r;
                }
            }
        } finally {
            lock.unlockWrite(stamp);
            afterUpdate();
        }
    }

    public KvResult mkdir(long index, ByteArray key) {
        return checkAndPut(index, key, null, true);
    }

    private void updateMinMax() {
        long max = 0;
        long min = Long.MAX_VALUE;
        for (Snapshot s : openSnapshots) {
            long idx = s.getSnapshotInfo().getLastIncludedIndex();
            max = Math.max(max, idx);
            min = Math.min(min, idx);
        }
        if (min == Long.MAX_VALUE) {
            min = 0;
        }
        maxOpenSnapshotIndex = max;
        minOpenSnapshotIndex = min;
    }

    void openSnapshot(Snapshot snapshot) {
        openSnapshots.add(snapshot);
        updateMinMax();
    }

    void closeSnapshot(Snapshot snapshot) {
        openSnapshots.remove(snapshot);
        updateMinMax();
    }

    static ByteArray parentKey(ByteArray key) {
        int lastIndexOfSep = key.lastIndexOf(KvClientConfig.SEPARATOR);
        if (lastIndexOfSep > 0) {
            return key.sub(0, lastIndexOfSep);
        } else {
            return ByteArray.EMPTY;
        }
    }

    // not update updateTime field and parent nodes, not fire watch event, raft index is not used
    public KvResult updateTtl(long index, ByteArray key) {
        long newTtlMillis = opContext.ttlMillis;
        if (newTtlMillis <= 0) {
            return new KvResult(KvCodes.INVALID_TTL);
        }
        int ck = checkKey(key, false, false);
        if (ck != KvCodes.SUCCESS) {
            return new KvResult(ck);
        }
        KvNodeHolder h = map.get(key);
        KvResult r = checkExistNode(h, opContext);
        if (r != null) {
            return r;
        }
        // no need to lock, because readers not check ttl
        ttlManager.updateTtl(index, key, h.latest, opContext);
        return KvResult.SUCCESS;
    }

    public KvResult updateLockLease(long index, ByteArray key) {
        int ck = checkKey(key, false, false);
        if (ck != KvCodes.SUCCESS) {
            return new KvResult(ck);
        }
        long newTtlMillis = opContext.ttlMillis;
        if (newTtlMillis <= 0) {
            return new KvResult(KvCodes.INVALID_TTL);
        }

        KvNodeHolder ph = map.get(key);
        KvResult r = checkExistNode(ph, opContext);
        if (r != null) {
            return r;
        }

        ByteArray subKey = KvServerUtil.buildLockKey(key, opContext.operator.getMostSignificantBits(),
                opContext.operator.getLeastSignificantBits());

        // check node sub node
        KvNodeHolder sh = map.get(subKey);
        if (ph.latest.peekNextOwner() != sh) {
            return new KvResult(KvCodes.LOCK_BY_OTHER);
        }

        // no need to lock, because readers not check ttl
        ttlManager.updateTtl(index, subKey, sh.latest, opContext);
        return KvResult.SUCCESS;
    }

    public KvResult expire(long index, ByteArray key, long expectRaftIndex) {
        int ck = checkKey(key, false, false);
        if (ck != KvCodes.SUCCESS) {
            return new KvResult(ck);
        }
        KvNodeHolder h = map.get(key);
        if (h == null || h.latest.removed) {
            if (log.isDebugEnabled()) {
                log.debug("key {} is already removed", key);
            }
            return KvResult.NOT_FOUND;
        }
        if (h.latest.ttlInfo == null || h.latest.ttlInfo.raftIndex != expectRaftIndex) {
            // the node is not the one we want to expire (maybe added after delete same key)
            if (log.isDebugEnabled()) {
                log.debug("key {} is already removed and re-add", key);
            }
            return new KvResult(KvCodes.TTL_INDEX_MISMATCH);
        }
        long t = lock.writeLock();
        try {
            return expireInLock(index, h);
        } finally {
            lock.unlockWrite(t);
            afterUpdate();
        }
    }

    // helper to read first 8 bytes of data as big-endian long; returns 0 if data too short or null
    private static long readHoldTtlMillis(byte[] data) {
        if (data == null || data.length < 8) {
            return 0L;
        }
        return ((long) (data[0] & 0xFF) << 56) |
                ((long) (data[1] & 0xFF) << 48) |
                ((long) (data[2] & 0xFF) << 40) |
                ((long) (data[3] & 0xFF) << 32) |
                ((long) (data[4] & 0xFF) << 24) |
                ((long) (data[5] & 0xFF) << 16) |
                ((long) (data[6] & 0xFF) << 8) |
                ((long) (data[7] & 0xFF));
    }


    private static final long MAX_TTL_MILLIS = TimeUnit.DAYS.toMillis(100 * 365);

    static String checkTtl(long ttl, byte[] data, boolean lock) {
        if (ttl > MAX_TTL_MILLIS) {
            return "ttl too large: " + ttl;
        }
        if (lock) {
            if (ttl < 0) {
                return "ttl must be non-negative: " + ttl;
            }
            if (data == null || data.length < 8) {
                return "no hold ttl";
            }
            long holdTtl = readHoldTtlMillis(data);
            if (holdTtl <= 0) {
                return "hold ttl must be positive: " + holdTtl;
            } else if (holdTtl > MAX_TTL_MILLIS) {
                return "hold ttl too large: " + holdTtl;
            } else if (holdTtl < ttl) {
                return "hold ttl " + holdTtl + " less than ttl " + ttl;
            }
        } else {
            if (ttl <= 0) {
                return "ttl must be positive: " + ttl;
            }
        }

        return null;
    }

    private KvResult expireInLock(long index, KvNodeHolder h) {
        if ((h.latest.flag & KvNode.FLAG_DIR_MASK) != 0) {
            // is dir
            ArrayDeque<KvNodeHolder> stack = new ArrayDeque<>();
            ArrayDeque<KvNodeHolder> output = new ArrayDeque<>();
            stack.push(h);
            while (!stack.isEmpty()) {
                KvNodeHolder current = stack.pop();
                output.push(current);
                if ((current.latest.flag & KvNode.FLAG_DIR_MASK) != 0) {
                    // the children map has no removed nodes, see doRemoveInLock
                    for (KvNodeHolder child : current.latest.childrenValues()) {
                        stack.push(child);
                    }
                }
            }
            while (!output.isEmpty()) {
                KvNodeHolder current = output.pop();
                if (current.latest.removed) {
                    BugLog.logAndThrow("removed node in dir children list");
                }
                if ((current.latest.flag & KvNode.FLAG_LOCK_MASK) != 0) {
                    BugLog.logAndThrow("lock node in temp dir");
                }
                doRemoveInLock(index, current);
            }
        } else {
            // not dir
            boolean isLock = (h.latest.flag & KvNode.FLAG_LOCK_MASK) != 0;
            KvNodeHolder parent = h.parent;
            if (parent.latest.removed) {
                BugLog.getLog().error("parent removed");
            }
            if (isLock && (parent.latest.flag & KvNode.FLAG_LOCK_MASK) == 0) {
                BugLog.getLog().error("parent has no lock mask");
            }
            boolean ownersLock = isLock && h == parent.latest.peekNextOwner();
            doRemoveInLock(index, h);
            if (isLock) {
                // KvNodeEx.children has no removed nodes
                if (parent.latest.childCount() == 0) {
                    doRemoveInLock(index, parent);
                } else if (ownersLock) {
                    return updateNextOwnerIfExists(index, parent);
                }
            }
        }
        return KvResult.SUCCESS;
    }

    private KvResult updateNextOwnerIfExists(long index, KvNodeHolder parent) {
        KvNodeHolder nextLockOwner = parent.latest.peekNextOwner();
        if (nextLockOwner != null) {
            // update owner hold timeout
            KvNodeEx n = nextLockOwner.latest;
            long newHoldTtlMillis = readHoldTtlMillis(n.data);


            ts.refresh(1);
            long localCreateNanos = n.ttlInfo.expireNanos - n.ttlInfo.ttlMillis * 1_000_000L;
            // max serverSideWaitNanos error is 1ms (1_000_000L), so subtract it
            long serverSideWaitNanos = Math.max(0, ts.nanoTime - localCreateNanos - 1_000_000L);

            // NOTICE here re-use the opContext, so the old context is overwritten and should not be used later.
            // re-init opContext so owner/ttlMillis are set appropriately.
            opContext.init(DtKV.BIZ_TYPE_EXPIRE, n.ttlInfo.owner, newHoldTtlMillis,
                    opContext.leaderCreateTimeMillis, opContext.localCreateNanos);
            ttlManager.updateTtl(index, nextLockOwner.key, n, opContext);
            return new KvResultWithNewOwnerInfo(KvCodes.SUCCESS, n, serverSideWaitNanos);
        }
        return KvResult.SUCCESS;
    }

    public KvResult tryLock(long index, ByteArray key, byte[] data) {
        long ttlMillis = opContext.ttlMillis;
        opContext.ttlMillis = 0; // the lock dir has no ttl
        long stamp = lock.writeLock();
        try {
            KvResult r = checkAndPut(index, key, null, false);
            if (r.getBizCode() != KvCodes.SUCCESS && r.getBizCode() != KvCodes.DIR_EXISTS) {
                return r;
            }
            opContext.ttlMillis = ttlMillis; // restore ttl
            KvNodeHolder parent = lastPutNodeHolder;
            ByteArray fullKey = KvServerUtil.buildLockKey(parent.key,
                    opContext.operator.getMostSignificantBits(), opContext.operator.getLeastSignificantBits());
            KvNodeHolder sub = map.get(fullKey);
            KvNodeHolder oldOwner = parent.latest.peekNextOwner();
            if (opContext.ttlMillis == 0 && oldOwner != null && oldOwner != sub) {
                // tryLock and has lock owner
                return new KvResult(KvCodes.LOCK_BY_OTHER);
            }
            return doPutInLock(index, fullKey, data, sub, parent, parent.key.length);
        } finally {
            lock.unlockWrite(stamp);
            afterUpdate();
        }
    }

    public KvResult unlock(long index, ByteArray key) {
        int ck = checkKey(key, false, false);
        if (ck != KvCodes.SUCCESS) {
            return new KvResult(ck);
        }
        KvNodeHolder parent = map.get(key);
        KvResult r = checkExistNode(parent, opContext);
        if (r != null) {
            return r;
        }
        ByteArray fullKey = KvServerUtil.buildLockKey(parent.key,
                opContext.operator.getMostSignificantBits(), opContext.operator.getLeastSignificantBits());
        KvNodeHolder sub = map.get(fullKey);
        if (sub == null || sub.latest.removed) {
            return new KvResult(KvCodes.LOCK_BY_OTHER);
        }
        if (sub.parent != parent) {
            BugLog.logAndThrow("sub.parent != parent");
        }
        boolean holdLock = sub == parent.latest.peekNextOwner();
        long stamp = lock.writeLock();
        try {
            doRemoveInLock(index, sub);
            boolean removeParent = parent.latest.childCount() == 0;
            if (removeParent) {
                doRemoveInLock(index, parent);
            }
            if (holdLock) {
                if (removeParent) {
                    return KvResult.SUCCESS;
                } else {
                    // if parent is removed, the updateNextOwnerIfExists may cause problem
                    return updateNextOwnerIfExists(index, parent);
                }
            } else {
                return new KvResult(KvCodes.LOCK_BY_OTHER);
            }
        } finally {
            lock.unlockWrite(stamp);
            afterUpdate();
        }
    }
}
