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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class KvImpl {
    // When iterating over this map, we need to divide the process into multiple steps,
    // with each step only accessing a portion of the map. Therefore, ConcurrentHashMap is needed here.
    final ConcurrentHashMap<String, KvNodeHolder> map;


    // for fast access root dir
    private final KvNodeEx root;

    // write operations is not atomic, so we need lock although ConcurrentHashMap is used
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;

    private final Timestamp ts;

    long maxOpenSnapshotIndex = 0;
    long minOpenSnapshotIndex = 0;

    public KvImpl(Timestamp ts, int initCapacity, float loadFactor) {
        this.ts = ts;
        this.map = new ConcurrentHashMap<>(initCapacity, loadFactor);
        this.root = new KvNodeEx(0, 0, true, null);
        this.map.put("", new KvNodeHolder("", "", root));
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        readLock = lock.readLock();
        writeLock = lock.writeLock();
    }

    /**
     * This method may be called in other threads.
     * <p>
     * For simplification, this method reads the latest snapshot, rather than the one specified by
     * the raftIndex parameter, and this does not violate linearizability.
     */
    public KvResult get(@SuppressWarnings("unused") long raftIndex, String key) {
        if (key == null || key.isEmpty()) {
            return new KvResult(KvResult.CODE_KEY_IS_NULL);
        }
        readLock.lock();
        try {
            KvNodeHolder h = map.get(key);
            if (h == null) {
                return KvResult.NOT_FOUND;
            }
            KvNodeEx kvNode = h.latest;
            if (kvNode.removeAtIndex > 0) {
                return KvResult.NOT_FOUND;
            }
            if (kvNode.dir) {
                return new KvResult(KvResult.CODE_NOT_VALUE);
            } else {
                KvResult r = new KvResult(KvResult.CODE_SUCCESS);
                r.setData(kvNode.data);
                return r;
            }
        } finally {
            readLock.unlock();
        }
    }

    public KvResult put(long index, String key, byte[] data) {
        if (key == null || key.isEmpty()) {
            return new KvResult(KvResult.CODE_KEY_IS_NULL);
        }
        if (data == null || data.length == 0) {
            return new KvResult(KvResult.CODE_VALUE_IS_NULL);
        }
        if (key.charAt(0) == '.' || key.charAt(key.length() - 1) == '.') {
            return new KvResult(KvResult.CODE_INVALID_KEY);
        }
        KvNodeEx dir;
        int lastIndexOfDot = key.lastIndexOf('.');
        if (lastIndexOfDot > 0) {
            String dirKey = key.substring(0, lastIndexOfDot);
            KvNodeHolder h = map.get(dirKey);
            dir = h == null ? null : h.latest;
            if (dir == null || dir.removeAtIndex > 0) {
                return new KvResult(KvResult.CODE_DIR_NOT_EXISTS);
            }
            if (!dir.dir) {
                return new KvResult(KvResult.CODE_NOT_DIR);
            }
        } else {
            dir = root;
        }
        long now = ts.getWallClockMillis();
        writeLock.lock();
        try {
            KvNodeHolder h = map.get(key);
            boolean overwrite;
            if (h == null) {
                String keyInDir = key.substring(lastIndexOfDot + 1);
                KvNodeEx newKvNode = new KvNodeEx(index, now, false, data);
                h = new KvNodeHolder(key, keyInDir, newKvNode);
                map.put(key, h);
                dir.children.put(keyInDir, h);
                overwrite = false;
            } else {
                overwrite = h.latest.removeAtIndex == 0;
                KvNodeEx newKvNode;
                if (overwrite) {
                    newKvNode = new KvNodeEx(h.latest.createIndex, h.latest.createTime, false, data);
                } else {
                    newKvNode = new KvNodeEx(index, now, false, data);
                }
                newKvNode.updateIndex = index;
                newKvNode.updateTime = now;
                newKvNode.previous = h.latest;
                h.latest = newKvNode;
            }
            gc(h);
            return overwrite ? KvResult.SUCCESS_OVERWRITE : KvResult.SUCCESS;
        } finally {
            writeLock.unlock();
        }
    }

    private void gc(KvNodeHolder h) {
        KvNodeEx n = h.latest;
        if (maxOpenSnapshotIndex > 0) {
            KvNodeEx parent = null;
            while (n != null) {
                if (n.createIndex > maxOpenSnapshotIndex && (parent != null || n.removeAtIndex > 0)) {
                    // n is not needed
                    if (parent == null) {
                        if (n.previous == null) {
                            removeFromMap(h);
                            return;
                        } else {
                            h.latest = n.previous;
                        }
                    } else {
                        parent.previous = n.previous;
                    }
                } else if ((parent != null && parent.createIndex <= minOpenSnapshotIndex
                        && (parent.removeAtIndex == 0 || parent.removeAtIndex > minOpenSnapshotIndex))
                        || (n.removeAtIndex > 0 && n.removeAtIndex <= minOpenSnapshotIndex)) {
                    if (parent == null) {
                        removeFromMap(h);
                    } else {
                        parent.previous = null;
                    }
                    return;
                }
                parent = n;
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

    public Supplier<Boolean> gc() {
        Iterator<Map.Entry<String, KvNodeHolder>> it = map.entrySet().iterator();
        return () -> {
            writeLock.lock();
            try {
                for (int i = 0; i < 3000; i++) {
                    if (!it.hasNext()) {
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
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null");
        }
        KvNodeHolder h = map.get(key);
        if (h == null) {
            return KvResult.NOT_FOUND;
        }
        KvNodeEx n = h.latest;
        if (n.dir && !n.children.isEmpty()) {
            for (Map.Entry<String, KvNodeHolder> e : n.children.entrySet()) {
                KvNodeEx child = e.getValue().latest;
                if (child.removeAtIndex == 0) {
                    return new KvResult(KvResult.CODE_HAS_CHILDREN);
                }
            }
        }
        KvResult r;
        if (n.removeAtIndex == 0) {
            n.removeAtIndex = index;
            r = KvResult.SUCCESS;
        } else {
            r = KvResult.NOT_FOUND;
        }
        writeLock.lock();
        try {
            gc(h);
        } finally {
            writeLock.unlock();
        }
        return r;
    }
}
