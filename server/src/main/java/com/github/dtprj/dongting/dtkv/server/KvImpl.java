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

import com.github.dtprj.dongting.buf.RefBuffer;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author huangli
 */
class KvImpl {
    private final ConcurrentHashMap<String, Value> map = new ConcurrentHashMap<>();
    private final LinkedList<Value> needCleanList = new LinkedList<>();

    public RefBuffer get(long index, String key) {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        Value value = map.get(key);
        while (value != null) {
            if (value.raftIndex > index) {
                value = value.previous;
            } else {
                break;
            }
        }
        if (value == null) {
            return null;
        } else {
            return value.data;
        }
    }

    private RefBuffer release(Value v) {
        if (v == null) {
            return null;
        }
        RefBuffer rb = v.data;
        if (rb != null) {
            rb.release();
        }
        return rb;
    }

    public void put(long index, String key, RefBuffer data, long maxOpenSnapshotIndex) {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        if (data == null || data.getBuffer() == null || data.getBuffer().remaining() == 0) {
            throw new IllegalArgumentException("value is null");
        }
        data.retain();
        Value newValue = new Value(index, key, data);
        Value oldValue = map.put(key, newValue);
        if (oldValue != null) {
            newValue.key = oldValue.key;
        }
        if (maxOpenSnapshotIndex > 0) {
            while (oldValue != null && oldValue.raftIndex > maxOpenSnapshotIndex) {
                release(oldValue);
                oldValue = oldValue.previous;
            }
            newValue.previous = oldValue;
            if (oldValue != null && !oldValue.evicted) {
                oldValue.evicted = true;
                needCleanList.add(newValue);
            }
        } else {
            release(oldValue);
        }
    }

    public void gc() {
        LinkedList<Value> needCleanList = this.needCleanList;
        while (!needCleanList.isEmpty()) {
            Value v = needCleanList.removeFirst();
            Value previous = v.previous;
            release(previous);
            v.previous = null;
            if (v.data == null && !v.evicted) {
                map.remove(v.key);
            }
        }
    }

    public Boolean remove(long index, String key, long maxOpenSnapshotIndex) {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        Value oldValue = map.remove(key);
        if (maxOpenSnapshotIndex > 0) {
            if (oldValue == null) {
                return Boolean.FALSE;
            } else {
                boolean result = oldValue.data != null;
                while (oldValue != null && oldValue.raftIndex > maxOpenSnapshotIndex) {
                    release(oldValue);
                    oldValue = oldValue.previous;
                }
                if (oldValue != null) {
                    Value newValue = new Value(index, oldValue.key, null);
                    newValue.previous = oldValue;
                    oldValue.evicted = true;
                    map.put(key, newValue);
                    needCleanList.add(newValue);
                }
                return result;
            }
        } else {
            return release(oldValue) != null;
        }
    }

    public Map<String, Value> getMap() {
        return map;
    }
}
