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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author huangli
 */
class Kv {
    final ConcurrentSkipListMap<String, Value> map = new ConcurrentSkipListMap<>();

    public CompletableFuture<Object> get(String key) {
        if (key == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("key is null"));
        }
        Value value = map.get(key);
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        } else {
            return CompletableFuture.completedFuture(value.getData());
        }
    }

    public CompletableFuture<Object> put(long index, String key, byte[] data, long minOpenSnapshotIndex) {
        if (key == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("key is null"));
        }
        if (data == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("value is null"));
        }
        Value newValue = new Value(index, data);
        Value oldValue = map.put(key, newValue);
        if (minOpenSnapshotIndex != 0 && oldValue != null) {
            newValue.setPrevious(oldValue);
            gc(newValue, oldValue, minOpenSnapshotIndex);
        }
        return CompletableFuture.completedFuture(null);
    }

    private void gc(Value newValue, Value oldValue, long minOpenSnapshotIndex) {
        while (oldValue != null) {
            if (newValue.getRaftIndex() <= minOpenSnapshotIndex) {
                newValue.setPrevious(null);
            }
            newValue = oldValue;
            oldValue = oldValue.getPrevious();
        }
    }

    public CompletableFuture<Object> remove(long index, String key, long minOpenSnapshotIndex) {
        if (key == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("key is null"));
        }
        Value oldValue = map.remove(key);
        if (minOpenSnapshotIndex == 0) {
            return CompletableFuture.completedFuture(oldValue != null && oldValue.getData() != null);
        } else {
            if (oldValue == null) {
                return CompletableFuture.completedFuture(false);
            } else {
                Value newValue = new Value(index, null);
                newValue.setPrevious(oldValue);
                map.put(key, newValue);
                gc(newValue, oldValue, minOpenSnapshotIndex);
                return CompletableFuture.completedFuture(oldValue.getData() != null);
            }
        }
    }
}
