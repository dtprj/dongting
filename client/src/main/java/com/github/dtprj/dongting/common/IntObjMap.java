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
package com.github.dtprj.dongting.common;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

/**
 * @author huangli
 */
public class IntObjMap<V> {
    private final IntObjectHashMap<V> map;

    public IntObjMap() {
        this.map = new IntObjectHashMap<>();
    }

    public V get(int key) {
        return map.get(key);
    }

    public V put(int key, V value) {
        return map.put(key, value);
    }

    public V remove(int key) {
        return map.remove(key);
    }

    public int size() {
        return map.size();
    }

    public void forEach(Visitor<V> visitor) {
        for (IntObjectCursor<V> en : map) {
            visitor.visit(en.key, en.value);
        }
    }

    @FunctionalInterface
    public interface Visitor<V> {
        void visit(int key, V value);
    }
}
