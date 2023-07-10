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

import java.util.LinkedList;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author huangli
 */
class Kv {
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

    public void put(long index, String key, byte[] data, long minOpenSnapshotIndex) {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        if (data == null) {
            throw new IllegalArgumentException("value is null");
        }
        Value newValue = new Value(index, data);
        Value oldValue = map.put(key, newValue);
        if (minOpenSnapshotIndex != 0 && oldValue != null) {
            newValue.setPrevious(oldValue);
            needCleanList.add(newValue);
        }
        gc(minOpenSnapshotIndex);
    }

    private void gc(long minOpenSnapshotIndex) {
        Value value;
        LinkedList<Value> needCleanList = this.needCleanList;
        while ((value = needCleanList.peekFirst()) != null) {
            Value oldValue = value.getPrevious();
            if (oldValue.getRaftIndex() >= minOpenSnapshotIndex) {
                break;
            }
            value.setPrevious(null);
            needCleanList.removeFirst();
        }
    }

    public Boolean remove(long index, String key, long minOpenSnapshotIndex) {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        Value oldValue = map.remove(key);
        if (minOpenSnapshotIndex == 0) {
            gc(minOpenSnapshotIndex);
            return oldValue != null && oldValue.getData() != null;
        } else {
            if (oldValue == null) {
                gc(minOpenSnapshotIndex);
                return false;
            } else {
                Value newValue = new Value(index, null);
                newValue.setPrevious(oldValue);
                map.put(key, newValue);
                needCleanList.add(newValue);
                gc(minOpenSnapshotIndex);
                return oldValue.getData() != null;
            }
        }
    }

    public ConcurrentSkipListMap<String, Value> getMap() {
        return map;
    }
}
