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
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.sm.Snapshot;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class KvImpl {
    private static final DtLog log = DtLogs.getLogger(KvImpl.class);

    public static final byte SEPARATOR = '.';

    private static final int MAX_KEY_SIZE = 8 * 1024;
    private static final int MAX_VALUE_SIZE = 1024 * 1024;
    private static final int GC_ITEMS = 3000;

    // only update int unit test
    int maxKeySize = MAX_KEY_SIZE;
    int maxValueSize = MAX_VALUE_SIZE;
    int gcItems = GC_ITEMS;

    private final int groupId;

    // When iterating over this map, we need to divide the process into multiple steps,
    // with each step only accessing a portion of the map. Therefore, ConcurrentHashMap is needed here.
    final ConcurrentHashMap<ByteArray, KvNodeHolder> map;


    // for fast access root dir
    final KvNodeHolder root;

    // write operations is not atomic, so we need lock although ConcurrentHashMap is used
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;

    private final Timestamp ts;

    private final ArrayList<Snapshot> openSnapshots = new ArrayList<>();
    private long maxOpenSnapshotIndex = 0;
    private long minOpenSnapshotIndex = 0;

    public KvImpl(Timestamp ts, int groupId, int initCapacity, float loadFactor) {
        this.ts = ts;
        this.groupId = groupId;
        this.map = new ConcurrentHashMap<>(initCapacity, loadFactor);
        KvNodeEx n = new KvNodeEx(0, 0, 0, 0, true, null);
        this.root = new KvNodeHolder(ByteArray.EMPTY, ByteArray.EMPTY, n, null);
        this.map.put(ByteArray.EMPTY, root);
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        readLock = lock.readLock();
        writeLock = lock.writeLock();
    }

    int checkKey(ByteArray key, boolean allowEmpty, boolean fullCheck) {
        if (key != null && key.isSlice()) {
            // slice key is not allowed
            return KvCodes.CODE_INVALID_KEY;
        }
        byte[] bs = key == null ? null : key.getData();
        if (bs == null || bs.length == 0) {
            if (allowEmpty) {
                return KvCodes.CODE_SUCCESS;
            } else {
                return KvCodes.CODE_INVALID_KEY;
            }
        }
        if (bs.length > maxKeySize) {
            return KvCodes.CODE_KEY_TOO_LONG;
        }
        if (bs[0] == SEPARATOR || bs[bs.length - 1] == SEPARATOR) {
            return KvCodes.CODE_INVALID_KEY;
        }
        if (fullCheck) {
            int lastSep = -1;
            for (int len = bs.length, i = 0; i < len; i++) {
                if (bs[i] == SEPARATOR) {
                    if (lastSep == i - 1) {
                        return KvCodes.CODE_INVALID_KEY;
                    }
                    lastSep = i;
                }
            }
        }
        return KvCodes.CODE_SUCCESS;
    }

    /**
     * This method may be called in other threads.
     * <p>
     * For simplification, this method reads the latest snapshot, rather than the one specified by
     * the raftIndex parameter, and this does not violate linearizability.
     */
    public KvResult get(ByteArray key) {
        int ck = checkKey(key, true, false);
        if (ck != KvCodes.CODE_SUCCESS) {
            return new KvResult(ck);
        }
        readLock.lock();
        try {
            return get0(key);
        } finally {
            readLock.unlock();
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
        return new KvResult(KvCodes.CODE_SUCCESS, kvNode);
    }

    /**
     * This method may be called in other threads.
     * <p>
     * For simplification, this method reads the latest snapshot, rather than the one specified by
     * the raftIndex parameter, and this does not violate linearizability.
     */
    public Pair<Integer, List<KvResult>> batchGet(List<byte[]> keys) {
        if (keys == null || keys.isEmpty()) {
            return new Pair<>(KvCodes.CODE_INVALID_KEY, null);
        }
        int s = keys.size();
        ArrayList<KvResult> list = new ArrayList<>(s);
        readLock.lock();
        try {
            for (int i = 0; i < s; i++) {
                byte[] bs = keys.get(i);
                ByteArray key = bs == null ? null : new ByteArray(bs);
                int ck = checkKey(key, true, false);
                if (ck != KvCodes.CODE_SUCCESS) {
                    list.add(new KvResult(ck));
                } else {
                    list.add(get0(key));
                }
            }
        } finally {
            readLock.unlock();
        }
        return new Pair<>(KvCodes.CODE_SUCCESS, list);
    }

    /**
     * This method may be called in other threads.
     * <p>
     * For simplification, this method reads the latest snapshot, rather than the one specified by
     * the raftIndex parameter, and this does not violate linearizability.
     */
    public Pair<Integer, List<KvResult>> list(ByteArray key) {
        int ck = checkKey(key, true, false);
        if (ck != KvCodes.CODE_SUCCESS) {
            return new Pair<>(ck, null);
        }
        readLock.lock();
        boolean linked;
        List<KvResult> list;
        try {
            KvNodeHolder h;
            if (key == null || key.getData().length == 0) {
                h = root;
            } else {
                h = map.get(key);
            }
            if (h == null) {
                return new Pair<>(KvCodes.CODE_NOT_FOUND, null);
            }
            KvNodeEx kvNode = h.latest;
            if (kvNode.removed) {
                return new Pair<>(KvCodes.CODE_NOT_FOUND, null);
            }
            if (!kvNode.isDir()) {
                return new Pair<>(KvCodes.CODE_PARENT_NOT_DIR, null);
            }
            if (kvNode.children.size() > 10) {
                linked = true;
                list = new LinkedList<>();
            } else {
                linked = false;
                list = new ArrayList<>();
            }
            for (KvNodeHolder child : kvNode.children.values()) {
                if (!child.latest.removed) {
                    list.add(new KvResult(KvCodes.CODE_SUCCESS, child.latest, child.keyInDir));
                }
            }
        } finally {
            readLock.unlock();
        }
        if (linked) {
            // encode should use random access list
            return new Pair<>(KvCodes.CODE_SUCCESS, new ArrayList<>(list));
        } else {
            return new Pair<>(KvCodes.CODE_SUCCESS, list);
        }
    }

    public KvResult put(long index, ByteArray key, byte[] data) {
        if (data == null || data.length == 0) {
            return new KvResult(KvCodes.CODE_INVALID_VALUE);
        }
        if (data.length > maxValueSize) {
            return new KvResult(KvCodes.CODE_VALUE_TOO_LONG);
        }
        return doPut(index, key, data, true);
    }

    private KvResult doPut(long index, ByteArray key, byte[] data, boolean lock) {
        int ck = checkKey(key, false, false);
        if (ck != KvCodes.CODE_SUCCESS) {
            return new KvResult(ck);
        }
        KvNodeHolder parent;
        int lastIndexOfSep = key.lastIndexOf(SEPARATOR);
        if (lastIndexOfSep > 0) {
            ByteArray dirKey = key.sub(0, lastIndexOfSep);
            parent = map.get(dirKey);
            if (parent == null || parent.latest.removed) {
                return new KvResult(KvCodes.CODE_PARENT_DIR_NOT_EXISTS);
            }
            if (!parent.latest.isDir()) {
                return new KvResult(KvCodes.CODE_PARENT_NOT_DIR);
            }
        } else {
            parent = root;
        }
        KvNodeHolder h = map.get(key);
        if (lock) {
            writeLock.lock();
        }
        try {
            return doPut(index, key, data, h, parent, lastIndexOfSep);
        } finally {
            if (lock) {
                writeLock.unlock();
            }
        }
    }

    private KvResult doPut(long index, ByteArray key, byte[] data, KvNodeHolder current,
                           KvNodeHolder parent, int lastIndexOfSep) {
        KvResult result;
        long timestamp = ts.getWallClockMillis();
        boolean newValueIsDir = data == null || data.length == 0;
        if (current == null) {
            ByteArray keyInDir = key.sub(lastIndexOfSep + 1);
            KvNodeEx newKvNode = new KvNodeEx(index, timestamp, index, timestamp, newValueIsDir, data);
            current = new KvNodeHolder(key, keyInDir, newKvNode, parent);
            map.put(key, current);
            parent.latest.children.put(keyInDir, current);
            result = KvResult.SUCCESS;
        } else {
            KvNodeEx oldNode = current.latest;
            KvNodeEx newKvNode;
            if (oldNode.removed) {
                newKvNode = new KvNodeEx(index, timestamp, index, timestamp, newValueIsDir, data);
                result = KvResult.SUCCESS;
            } else {
                // override
                boolean oldValueIsDir = oldNode.isDir();
                if (newValueIsDir != oldValueIsDir) {
                    return new KvResult(oldValueIsDir ? KvCodes.CODE_DIR_EXISTS : KvCodes.CODE_VALUE_EXISTS);
                }
                newKvNode = new KvNodeEx(oldNode.getCreateIndex(), oldNode.getCreateTime(),
                        index, timestamp, newValueIsDir, data);
                result = KvResult.SUCCESS_OVERWRITE;
            }
            if (maxOpenSnapshotIndex > 0) {
                newKvNode.previous = oldNode;
                current.latest = newKvNode;
                gc(current);
            } else {
                current.latest = newKvNode;
            }
        }
        updateParent(index, timestamp, parent);
        return result;
    }

    public Pair<Integer, List<KvResult>> batchPut(long index, List<byte[]> keys, List<byte[]> values) {
        if (keys == null || keys.isEmpty()) {
            return new Pair<>(KvCodes.CODE_INVALID_KEY, null);
        }
        int size = keys.size();
        ArrayList<KvResult> list = new ArrayList<>(size);
        if (values == null || values.size() != size) {
            return new Pair<>(KvCodes.CODE_INVALID_VALUE, null);
        }
        writeLock.lock();
        try {
            for (int i = 0; i < size; i++) {
                byte[] k = keys.get(i);
                list.add(doPut(index, k == null ? null : new ByteArray(k), values.get(i), false));
            }
        } finally {
            writeLock.unlock();
        }
        return new Pair<>(KvCodes.CODE_SUCCESS, list);
    }

    private void updateParent(long index, long timestamp, KvNodeHolder parent) {
        while (parent != null) {
            KvNodeEx oldDirNode = parent.latest;
            parent.latest = new KvNodeEx(oldDirNode, index, timestamp);
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
                if (next != null && n.getUpdateIndex() > maxOpenSnapshotIndex) {
                    next.previous = n.previous;
                } else if (next != null && next.getUpdateIndex() <= minOpenSnapshotIndex) {
                    next.previous = null;
                    return;
                } else if (n.removed) {
                    KvNodeEx p;
                    while ((p = n.previous) != null && (p.getUpdateIndex() > maxOpenSnapshotIndex
                            || n.getUpdateIndex() <= minOpenSnapshotIndex)) {
                        n.previous = p.previous;
                    }
                    if (p == null) {
                        if (next == null) {
                            removeFromMap(h);
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
                removeFromMap(h);
            } else {
                n.previous = null;
            }
        }
    }

    private void removeFromMap(KvNodeHolder h) {
        map.remove(h.key);
        h.parent.latest.children.remove(h.keyInDir);
    }

    void installSnapshotPut(EncodeStatus encodeStatus) {
        // do not need lock, no other requests during install snapshot
        KvNodeEx n = new KvNodeEx(encodeStatus.createIndex, encodeStatus.createTime, encodeStatus.updateIndex,
                encodeStatus.updateTime, encodeStatus.valueBytes == null || encodeStatus.valueBytes.length == 0,
                encodeStatus.valueBytes);
        if (encodeStatus.keyBytes == null || encodeStatus.keyBytes.length == 0) {
            root.latest = n;
        } else {
            KvNodeHolder parent;
            ByteArray key = new ByteArray(encodeStatus.keyBytes);
            ByteArray keyInDir;
            int lastIndexOfSep = key.lastIndexOf(SEPARATOR);
            if (lastIndexOfSep == -1) {
                parent = root;
                keyInDir = key;
            } else {
                ByteArray dirKey = key.sub(0, lastIndexOfSep);
                parent = map.get(dirKey);
                keyInDir = key.sub(lastIndexOfSep + 1);
            }
            KvNodeHolder h = new KvNodeHolder(key, keyInDir, n, parent);
            parent.latest.children.put(keyInDir, h);
            map.put(key, h);
        }
    }

    public Supplier<Boolean> createGcTask(Supplier<Boolean> cancel) {
        Iterator<KvNodeHolder> it = map.values().iterator();
        long t = System.currentTimeMillis();
        log.info("group {} start gc task", groupId);
        return () -> {
            if (cancel.get()) {
                return Boolean.FALSE;
            }
            writeLock.lock();
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
                writeLock.unlock();
            }
        };
    }

    public KvResult remove(long index, ByteArray key) {
        return doRemove(index, key, true);
    }

    private KvResult doRemove(long index, ByteArray key, boolean lock) {
        int ck = checkKey(key, false, false);
        if (ck != KvCodes.CODE_SUCCESS) {
            return new KvResult(ck);
        }
        KvNodeHolder h = map.get(key);
        if (h == null) {
            return KvResult.NOT_FOUND;
        }
        KvNodeEx n = h.latest;
        if (n.removed) {
            return KvResult.NOT_FOUND;
        }
        if (n.isDir() && !n.children.isEmpty()) {
            for (KvNodeHolder c : n.children.values()) {
                KvNodeEx child = c.latest;
                if (!child.removed) {
                    return new KvResult(KvCodes.CODE_HAS_CHILDREN);
                }
            }
        }
        if (lock) {
            writeLock.lock();
        }
        try {
            return doRemoveInLock(index, h);
        } finally {
            if (lock) {
                writeLock.unlock();
            }
        }
    }

    private KvResult doRemoveInLock(long index, KvNodeHolder h) {
        if (maxOpenSnapshotIndex > 0) {
            KvNodeEx n = h.latest;
            KvNodeEx newKvNode = new KvNodeEx(n.getCreateIndex(), n.getCreateTime(), index,
                    ts.getWallClockMillis(), n.isDir(), null);
            newKvNode.removed = true;
            h.latest = newKvNode;
            newKvNode.previous = n;
            gc(h);
        } else {
            removeFromMap(h);
        }
        updateParent(index, ts.getWallClockMillis(), h.parent);
        return KvResult.SUCCESS;
    }

    public Pair<Integer, List<KvResult>> batchRemove(long index, List<byte[]> keys) {
        if (keys == null || keys.isEmpty()) {
            return new Pair<>(KvCodes.CODE_INVALID_KEY, null);
        }
        int size = keys.size();
        ArrayList<KvResult> list = new ArrayList<>(size);
        writeLock.lock();
        try {
            for (int i = 0; i < size; i++) {
                byte[] k = keys.get(i);
                list.add(doRemove(index, k == null ? null : new ByteArray(k), false));
            }
        } finally {
            writeLock.unlock();
        }
        return new Pair<>(KvCodes.CODE_SUCCESS, list);
    }

    public KvResult compareAndSet(long index, ByteArray key, byte[] expectedValue, byte[] newValue) {
        int ck = checkKey(key, false, false);
        if (ck != KvCodes.CODE_SUCCESS) {
            return new KvResult(ck);
        }
        KvNodeHolder parent;
        int lastIndexOfSep = key.lastIndexOf(SEPARATOR);
        if (lastIndexOfSep > 0) {
            ByteArray dirKey = key.sub(0, lastIndexOfSep);
            parent = map.get(dirKey);
            if (parent == null || parent.latest.removed) {
                return new KvResult(KvCodes.CODE_PARENT_DIR_NOT_EXISTS);
            }
            if (!parent.latest.isDir()) {
                return new KvResult(KvCodes.CODE_PARENT_NOT_DIR);
            }
        } else {
            parent = root;
        }
        KvNodeHolder h = map.get(key);
        writeLock.lock();
        try {
            if (expectedValue == null || expectedValue.length == 0) {
                if (h == null || h.latest.removed) {
                    KvResult r = doPut(index, key, newValue, h, parent, lastIndexOfSep);
                    return r == KvResult.SUCCESS_OVERWRITE ? KvResult.SUCCESS : r;
                } else {
                    return new KvResult(KvCodes.CODE_CAS_MISMATCH);
                }
            } else {
                if (h == null) {
                    return new KvResult(KvCodes.CODE_CAS_MISMATCH);
                }
                KvNodeEx n = h.latest;
                if (n.removed || n.isDir()) {
                    return new KvResult(KvCodes.CODE_CAS_MISMATCH);
                }
                byte[] bs = n.getData();
                if (bs == null || bs.length != expectedValue.length) {
                    return new KvResult(KvCodes.CODE_CAS_MISMATCH);
                }
                for (int i = 0; i < bs.length; i++) {
                    if (bs[i] != expectedValue[i]) {
                        return new KvResult(KvCodes.CODE_CAS_MISMATCH);
                    }
                }
                if (newValue == null || newValue.length == 0) {
                    return doRemoveInLock(index, h);
                } else {
                    KvResult r = doPut(index, key, newValue, h, parent, lastIndexOfSep);
                    return r == KvResult.SUCCESS_OVERWRITE ? KvResult.SUCCESS : r;
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public KvResult mkdir(long index, ByteArray key) {
        return doPut(index, key, null, true);
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

    void openSnapshot(KvSnapshot snapshot) {
        openSnapshots.add(snapshot);
        updateMinMax();
    }

    void closeSnapshot(KvSnapshot snapshot) {
        openSnapshots.remove(snapshot);
        updateMinMax();
    }

    ByteArray parentKey(ByteArray key) {
        int lastIndexOfSep = key.lastIndexOf(SEPARATOR);
        if (lastIndexOfSep > 0) {
            return key.sub(0, lastIndexOfSep);
        } else {
            return ByteArray.EMPTY;
        }
    }
}
