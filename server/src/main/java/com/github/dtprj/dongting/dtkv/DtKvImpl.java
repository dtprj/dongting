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
public class DtKvImpl {
    private final ConcurrentSkipListMap<String, Value> map = new ConcurrentSkipListMap<>();


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

    public CompletableFuture<Object> put(long index, String key, byte[] data) {
        if (key == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("key is null"));
        }
        Value old = map.put(key, new Value(index, data));
        return CompletableFuture.completedFuture(old == null);
    }

    public CompletableFuture<Object> remove(String key) {
        if (key == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("key is null"));
        }
        Value old = map.remove(key);
        return CompletableFuture.completedFuture(old == null);
    }
}
