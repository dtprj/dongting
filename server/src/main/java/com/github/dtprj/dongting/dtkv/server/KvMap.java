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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author huangli
 */
class KvMap {
    // When iterating over this map, we need to divide the process into multiple steps,
    // with each step only accessing a portion of the map. Therefore, ConcurrentHashMap is needed here.
    private final ConcurrentHashMap<ByteArray, KvNodeHolder> map;

    public KvMap(int initCapacity, float loadFactor) {
        this.map = new ConcurrentHashMap<>(initCapacity, loadFactor);
    }

    public KvNodeHolder get(ByteArray key) {
        return map.get(key);
    }

    public KvNodeHolder put(ByteArray key, KvNodeHolder value) {
        return map.put(key, value);
    }

    public KvNodeHolder remove(ByteArray key) {
        return map.remove(key);
    }

    public int size() {
        return map.size();
    }

    public Iterator<KvNodeHolder> iterator() {
        return map.values().iterator();
    }
}
