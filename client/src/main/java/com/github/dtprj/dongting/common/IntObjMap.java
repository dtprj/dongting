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

import java.util.Objects;

/**
 * This class is not thread safe.
 *
 * @author huangli
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class IntObjMap<V> {
    private static final int MAX_ARRAY_SIZE = 1 << 30;
    private final float loadFactor;
    private IntMapNode[] values;
    private int size;
    private int resizeThreshold;
    private boolean readVisit;
    private boolean rwVisit;

    public IntObjMap() {
        this(8, 0.75f);
    }

    public IntObjMap(int initSize, float loadFactor) {
        DtUtil.checkPositive(initSize, "initSize");
        DtUtil.checkPositive(loadFactor, "loadFactor");
        resizeThreshold = BitUtil.nextHighestPowerOfTwo(initSize);
        this.loadFactor = loadFactor;
    }

    protected int hashCode(int v) {
        return v;
    }

    public V get(int key) {
        return find(key, false);
    }

    private V find(int key, boolean remove) {
        IntMapNode[] values = this.values;
        if (values == null) {
            return null;
        }
        int idx = hashCode(key) & (values.length - 1);
        IntMapNode<V> existData = values[idx];
        if (existData == null) {
            return null;
        }
        if (existData.getKey() == key) {
            if (remove) {
                IntMapNode<V> next = existData.getNext();
                values[idx] = next;
                size--;
            }
            return existData.getValue();
        }
        IntMapNode<V> next;
        while ((next = existData.getNext()) != null) {
            if (next.getKey() == key) {
                if (remove) {
                    existData.setNext(next.getNext());
                    size--;
                }
                return next.getValue();
            }
            existData = next;
        }
        return null;
    }

    public V put(int key, V value) {
        Objects.requireNonNull(value);
        if (readVisit || rwVisit) {
            throw new IllegalStateException("can modify the map during iteration");
        }
        IntMapNode[] values = resize();
        V r = put0(values, new IntMapNode<>(key, value), values.length - 1);
        if (r == null) {
            size++;
        }
        return r;
    }

    @SuppressWarnings("unchecked")
    private V put0(IntMapNode[] values, IntMapNode<V> mn, int mask) {
        int key = mn.getKey();
        int idx = hashCode(key) & mask;
        IntMapNode<V> existData = values[idx];
        if (existData == null) {
            values[idx] = mn;
            return null;
        } else {
            while (true) {
                if (existData.getKey() == key) {
                    V old = existData.getValue();
                    existData.setValue(mn.getValue());
                    return old;
                }
                IntMapNode<V> next = existData.getNext();
                if (next == null) {
                    existData.setNext(mn);
                    return null;
                } else {
                    existData = next;
                }
            }
        }
    }

    private IntMapNode[] resize() {
        int threshold = this.resizeThreshold;
        IntMapNode[] values = this.values;
        if (values == null) {
            values = new IntMapNode[threshold];
            this.resizeThreshold = (int) (loadFactor * threshold);
            this.values = values;
            return values;
        }
        if (size < threshold) {
            return values;
        }

        int oldArrayLength = values.length;
        if (oldArrayLength >= MAX_ARRAY_SIZE) {
            return values;
        }
        int newSize = values.length << 1;
        this.resizeThreshold = (int) (loadFactor * newSize);
        int mask = newSize - 1;
        IntMapNode[] newValues = new IntMapNode[newSize];
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < oldArrayLength; i++) {
            IntMapNode<V> mn = values[i];
            if (mn == null) {
                continue;
            }
            do {
                IntMapNode<V> next = mn.getNext();
                mn.setNext(null);
                put0(newValues, mn, mask);
                mn = next;
            } while (mn != null);
        }
        this.values = newValues;
        return newValues;
    }

    public V remove(int key) {
        if (readVisit || rwVisit) {
            throw new IllegalStateException("can modify the map during iteration");
        }
        return find(key, true);
    }

    public int size() {
        return size;
    }

    public void forEach(Visitor<V> visitor) {
        if (rwVisit) {
            throw new IllegalStateException("can not iterate the map during iteration");
        }
        rwVisit = true;
        try {
            forEach0(visitor, null);
        } finally {
            rwVisit = false;
        }
    }

    public void forEach(ReadOnlyVisitor<V> visitor) {
        readVisit = true;
        try {
            forEach0(null, visitor);
        } finally {
            readVisit = false;
        }
    }

    private void forEach0(Visitor<V> visitor, ReadOnlyVisitor<V> readOnlyVisitor) {
        IntMapNode[] values = this.values;
        if (values == null) {
            return;
        }
        int len = values.length;
        for (int i = 0; i < len; i++) {
            IntMapNode<V> mn = values[i];
            if (mn == null) {
                continue;
            }
            IntMapNode<V> prev = null;
            do {
                if (visit(visitor, readOnlyVisitor, mn.getKey(), mn.getValue())) {
                    prev = mn;
                } else {
                    if (prev == null) {
                        values[i] = mn.getNext();
                    } else {
                        prev.setNext(mn.getNext());
                    }
                    size--;
                }
                mn = mn.getNext();
            } while (mn != null);
        }
    }

    private boolean visit(Visitor<V> visitor, ReadOnlyVisitor<V> readOnlyVisitor, int key, V value) {
        if (readOnlyVisitor == null) {
            return visitor.visit(key, value);
        } else {
            readOnlyVisitor.visit(key, value);
            return true;
        }
    }

    @FunctionalInterface
    public interface Visitor<V> {
        /**
         * return ture if this K/V should keep in Map, else remove it
         */
        boolean visit(int key, V value);
    }

    @FunctionalInterface
    public interface ReadOnlyVisitor<V> {
        void visit(int key, V value);
    }

    public static <V> Pair<V, IntObjMap<V>> copyOnWritePut(IntObjMap<V> map, int key, V value) {
        IntObjMap<V> newMap = copyMap(map);
        V oldValue = newMap.put(key, value);
        return new Pair<>(oldValue, newMap);
    }

    public static <V> Pair<V, IntObjMap<V>> copyOnWriteRemove(IntObjMap<V> map, int key) {
        IntObjMap<V> newMap = copyMap(map);
        V oldValue = newMap.remove(key);
        return new Pair<>(oldValue, newMap);
    }

    private static <V> IntObjMap<V> copyMap(IntObjMap<V> map) {
        IntObjMap<V> newMap;
        IntMapNode[] values = map.values;
        if (values == null) {
            newMap = new IntObjMap<>(map.resizeThreshold, map.loadFactor);
        } else {
            if (map.size + 1 >= map.resizeThreshold && values.length < MAX_ARRAY_SIZE) {
                newMap = new IntObjMap<>(values.length << 1, map.loadFactor);
            } else {
                newMap = new IntObjMap<>(values.length, map.loadFactor);
            }
        }
        map.forEach(newMap::put);
        return newMap;
    }
}
