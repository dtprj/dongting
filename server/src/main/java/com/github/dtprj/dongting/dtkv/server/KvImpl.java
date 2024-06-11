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

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author huangli
 */
class KvImpl {
    private final ConcurrentSkipListMap<String, Value> map = new ConcurrentSkipListMap<>();
    private final LinkedList<Value> needCleanList = new LinkedList<>();

    public byte[] get(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        Value value = map.get(key);
        if (value == null) {
            return null;
        } else {
            return value.getData();
        }
    }

    public void put(long index, String key, byte[] data, long maxOpenSnapshotIndex) {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        if (data == null) {
            throw new IllegalArgumentException("value is null");
        }
        Value newValue = new Value(index, key, data);
        Value oldValue = map.put(key, newValue);
        if (maxOpenSnapshotIndex > 0) {
            while (oldValue != null && oldValue.getRaftIndex() > maxOpenSnapshotIndex) {
                oldValue = oldValue.getPrevious();
            }
            if (oldValue != null) {
                oldValue.setEvicted(true);
                newValue.setPrevious(oldValue);
                needCleanList.add(newValue);
            }
        }
    }

    public void gc() {
        LinkedList<Value> needCleanList = this.needCleanList;
        while (!needCleanList.isEmpty()) {
            Value v = needCleanList.removeFirst();
            v.setPrevious(null);
            if (v.getData() == null && !v.isEvicted()) {
                map.remove(v.getKey());
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
                boolean result = oldValue.getData() != null;
                while (oldValue != null && oldValue.getRaftIndex() > maxOpenSnapshotIndex) {
                    oldValue = oldValue.getPrevious();
                }
                if (oldValue != null) {
                    Value newValue = new Value(index, key, null);
                    newValue.setPrevious(oldValue);
                    oldValue.setEvicted(true);
                    map.put(key, newValue);
                    needCleanList.add(newValue);
                }
                return result;
            }
        } else {
            return oldValue != null && oldValue.getData() != null;
        }
    }

    public Map<String, Value> getMap() {
        return map;
    }
}
