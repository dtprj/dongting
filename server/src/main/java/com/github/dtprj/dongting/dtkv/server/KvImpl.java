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

import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class KvImpl {
    private static final DtLog log = DtLogs.getLogger(KvImpl.class);

    public static final char SEPARATOR = '.';
    private static final int MAX_KEY_SIZE = 8 * 1024;
    private static final int MAX_VALUE_SIZE = 1024 * 1024;

    private final int groupId;

    // When iterating over this map, we need to divide the process into multiple steps,
    // with each step only accessing a portion of the map. Therefore, ConcurrentHashMap is needed here.
    final ConcurrentHashMap<String, KvNodeHolder> map;


    // for fast access root dir
    final KvNodeHolder root;

    // write operations is not atomic, so we need lock although ConcurrentHashMap is used
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;

    private final Timestamp ts;

    long maxOpenSnapshotIndex = 0;
    long minOpenSnapshotIndex = 0;

    public KvImpl(Timestamp ts, int groupId, int initCapacity, float loadFactor) {
        this.ts = ts;
        this.groupId = groupId;
        this.map = new ConcurrentHashMap<>(initCapacity, loadFactor);
        KvNodeEx n = new KvNodeEx(0, 0, 0, 0, null);
        this.root = new KvNodeHolder("", "", n, null);
        this.map.put("", root);
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        readLock = lock.readLock();
        writeLock = lock.writeLock();
    }

    private KvResult checkKey(String key, boolean allowEmpty) {
        if (key.isEmpty()) {
            if (allowEmpty) {
                return null;
            } else {
                return new KvResult(KvCodes.CODE_KEY_IS_NULL);
            }
        }
        if (key.charAt(0) == SEPARATOR || key.charAt(key.length() - 1) == SEPARATOR) {
            return new KvResult(KvCodes.CODE_INVALID_KEY);
        }
        if (key.length() > MAX_KEY_SIZE) {
            return new KvResult(KvCodes.CODE_KEY_TOO_LONG);
        }
        return null;
    }

    /**
     * This method may be called in other threads.
     * <p>
     * For simplification, this method reads the latest snapshot, rather than the one specified by
     * the raftIndex parameter, and this does not violate linearizability.
     */
    public KvResult get(@SuppressWarnings("unused") long raftIndex, String key) {
        key = key == null ? "" : key.trim();
        KvResult r = checkKey(key, true);
        if (r != null) {
            return r;
        }
        readLock.lock();
        try {
            KvNodeHolder h;
            if (key.isEmpty()) {
                h = root;
            } else {
                h = map.get(key);
            }
            if (h == null) {
                return KvResult.NOT_FOUND;
            }
            KvNodeEx kvNode = h.latest;
            if (kvNode.removeAtIndex > 0) {
                return KvResult.NOT_FOUND;
            }
            r = new KvResult(KvCodes.CODE_SUCCESS, kvNode);
            return r;
        } finally {
            readLock.unlock();
        }
    }

    public KvResult put(long index, String key, byte[] data) {
        if (data == null || data.length == 0) {
            return new KvResult(KvCodes.CODE_VALUE_IS_NULL);
        }
        if (data.length > MAX_VALUE_SIZE) {
            return new KvResult(KvCodes.CODE_VALUE_TOO_LONG);
        }
        return doPut(index, key, data, true);
    }

    protected KvResult doPut(long index, String key, byte[] data, boolean lock) {
        key = key == null ? "" : key.trim();
        KvResult r = checkKey(key, false);
        if (r != null) {
            return r;
        }

        KvNodeHolder parent;
        int lastIndexOfSep = key.lastIndexOf(SEPARATOR);
        if (lastIndexOfSep > 0) {
            String dirKey = key.substring(0, lastIndexOfSep);
            parent = map.get(dirKey);
            if (parent == null || parent.latest.removeAtIndex > 0) {
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
            boolean overwrite;
            long timestamp = ts.getWallClockMillis();
            if (h == null) {
                String keyInDir = key.substring(lastIndexOfSep + 1);
                KvNodeEx newKvNode = new KvNodeEx(index, timestamp, index, timestamp, data);
                h = new KvNodeHolder(key, keyInDir, newKvNode, parent);
                map.put(key, h);
                parent.latest.children.put(keyInDir, h);
                overwrite = false;
            } else {
                overwrite = h.latest.removeAtIndex == 0;
                KvNodeEx newKvNode;
                if (overwrite) {
                    if (data == null || data.length == 0) {
                        // mkdir can't overwrite any value
                        return new KvResult(h.latest.isDir() ? KvCodes.CODE_DIR_EXISTS : KvCodes.CODE_VALUE_EXISTS);
                    }
                    newKvNode = new KvNodeEx(h.latest.getCreateIndex(), h.latest.getCreateTime(),
                            index, timestamp, data);
                } else {
                    newKvNode = new KvNodeEx(index, timestamp, index, timestamp, data);
                }
                h.latest = newKvNode;
                if (maxOpenSnapshotIndex > 0) {
                    newKvNode.previous = h.latest;
                    gc(h);
                }
            }
            updateParent(index, timestamp, parent);
            return overwrite ? KvResult.SUCCESS_OVERWRITE : KvResult.SUCCESS;
        } finally {
            if (lock) {
                writeLock.unlock();
            }
        }
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
            KvNodeEx newNode = null;
            while (n != null) {
                if (n.getCreateIndex() > maxOpenSnapshotIndex && (newNode != null || n.removeAtIndex > 0)) {
                    // n is not needed
                    if (newNode == null) {
                        if (n.previous == null) {
                            removeFromMap(h);
                            return;
                        } else {
                            h.latest = n.previous;
                        }
                    } else {
                        newNode.previous = n.previous;
                    }
                } else if ((newNode != null && newNode.getCreateIndex() <= minOpenSnapshotIndex
                        && (newNode.removeAtIndex == 0 || newNode.removeAtIndex > minOpenSnapshotIndex))
                        || (n.removeAtIndex > 0 && n.removeAtIndex <= minOpenSnapshotIndex)) {
                    if (newNode == null) {
                        removeFromMap(h);
                    } else {
                        newNode.previous = null;
                    }
                    return;
                }
                newNode = n;
                n = n.previous;
            }
        } else {
            if (n.removeAtIndex > 0) {
                removeFromMap(h);
            } else {
                n.previous = null;
            }
        }
    }

    private void removeFromMap(KvNodeHolder h) {
        map.remove(h.key);
        map.get(h.keyInDir).latest.children.remove(h.keyInDir);
    }

    void installSnapshotPut(EncodeStatus encodeStatus) {
        // do not need lock, no other requests during install snapshot
        KvNodeEx n = new KvNodeEx(encodeStatus.createIndex, encodeStatus.createTime, encodeStatus.createIndex,
                encodeStatus.updateTime, encodeStatus.valueBytes);
        if (encodeStatus.keyBytes.length == 0) {
            root.latest = n;
        } else {
            byte[] keyBytes = encodeStatus.keyBytes;
            int lastIndexOfSep = -1;
            for (int size = keyBytes.length, i = size - 1; i >= 1; i--) {
                if (keyBytes[i] == SEPARATOR) {
                    lastIndexOfSep = i;
                    break;
                }
            }
            KvNodeHolder parent;
            String key = new String(keyBytes);
            String keyInDir;
            if (lastIndexOfSep == -1) {
                parent = root;
                keyInDir = key;
            } else {
                String dirKey = new String(keyBytes, 0, lastIndexOfSep);
                parent = map.get(dirKey);
                keyInDir = new String(keyBytes, lastIndexOfSep + 1, keyBytes.length - lastIndexOfSep - 1);
            }
            KvNodeHolder h = new KvNodeHolder(key, keyInDir, n, parent);
            parent.latest.children.put(keyInDir, h);
            map.put(key, h);
        }
    }

    public Supplier<Boolean> createGcTask() {
        Iterator<Map.Entry<String, KvNodeHolder>> it = map.entrySet().iterator();
        long t = System.currentTimeMillis();
        log.info("group {} start gc task", groupId);
        return () -> {
            writeLock.lock();
            try {
                for (int i = 0; i < 3000; i++) {
                    if (!it.hasNext()) {
                        log.info("group {} gc task finished, cost {} ms", groupId, System.currentTimeMillis() - t);
                        return Boolean.FALSE;
                    }
                    KvNodeHolder h = it.next().getValue();
                    gc(h);
                }
                return Boolean.TRUE;
            } finally {
                writeLock.unlock();
            }
        };
    }

    public KvResult remove(long index, String key) {
        key = key == null ? "" : key.trim();
        KvResult r = checkKey(key, false);
        if (r != null) {
            return r;
        }
        KvNodeHolder h = map.get(key);
        if (h == null) {
            return KvResult.NOT_FOUND;
        }
        KvNodeEx n = h.latest;
        if (n.removeAtIndex > 0) {
            return KvResult.NOT_FOUND;
        }
        if (n.isDir() && !n.children.isEmpty()) {
            for (Map.Entry<String, KvNodeHolder> e : n.children.entrySet()) {
                KvNodeEx child = e.getValue().latest;
                if (child.removeAtIndex == 0) {
                    return new KvResult(KvCodes.CODE_HAS_CHILDREN);
                }
            }
        }
        writeLock.lock();
        try {
            n.removeAtIndex = index;
            gc(h);
            updateParent(index, ts.getWallClockMillis(), h.parent);
        } finally {
            writeLock.unlock();
        }
        return KvResult.SUCCESS;
    }

    public KvResult mkdir(long index, String key) {
        return doPut(index, key, null, true);
    }
}
