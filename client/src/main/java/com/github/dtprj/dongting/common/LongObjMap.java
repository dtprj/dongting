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
public class LongObjMap<V> {
    private static final int MAX_ARRAY_SIZE = 1 << 30;
    private final float loadFactor;
    private LongMapNode[] values;
    private int size;
    private int resizeThreshold;
    private boolean readVisit;
    private boolean rwVisit;

    public LongObjMap() {
        this(8, 0.75f);
    }

    public LongObjMap(int initSize, float loadFactor) {
        DtUtil.checkPositive(initSize, "initSize");
        DtUtil.checkPositive(loadFactor, "loadFactor");
        resizeThreshold = BitUtil.nextHighestPowerOfTwo(initSize);
        this.loadFactor = loadFactor;
    }

    protected int hashCode(long v) {
        return Long.hashCode(v);
    }

    public V get(long key) {
        return find(key, false);
    }

    private V find(long key, boolean remove) {
        LongMapNode[] values = this.values;
        if (values == null) {
            return null;
        }
        int idx = hashCode(key) & (values.length - 1);
        LongMapNode<V> existData = values[idx];
        if (existData == null) {
            return null;
        }
        if (existData.getKey() == key) {
            if (remove) {
                LongMapNode<V> next = existData.getNext();
                values[idx] = next;
                size--;
            }
            return existData.getValue();
        }
        LongMapNode<V> next;
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

    public V put(long key, V value) {
        Objects.requireNonNull(value);
        if (readVisit || rwVisit) {
            throw new IllegalStateException("can modify the map during iteration");
        }
        LongMapNode[] values = resize();
        V r = put0(values, new LongMapNode<>(key, value), values.length - 1);
        if (r == null) {
            size++;
        }
        return r;
    }

    private V put0(LongMapNode[] values, LongMapNode<V> mn, int mask) {
        long key = mn.getKey();
        int idx = hashCode(key) & mask;
        LongMapNode<V> existData = values[idx];
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
                LongMapNode<V> next = existData.getNext();
                if (next == null) {
                    existData.setNext(mn);
                    return null;
                } else {
                    existData = next;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private LongMapNode[] resize() {
        int threshold = this.resizeThreshold;
        LongMapNode[] values = this.values;
        if (values == null) {
            values = new LongMapNode[threshold];
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
        LongMapNode[] newValues = new LongMapNode[newSize];
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < oldArrayLength; i++) {
            LongMapNode<V> mn = values[i];
            if (mn == null) {
                continue;
            }
            do {
                LongMapNode<V> next = mn.getNext();
                mn.setNext(null);
                put0(newValues, mn, mask);
                mn = next;
            } while (mn != null);
        }
        this.values = newValues;
        return newValues;
    }

    public V remove(long key) {
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
        LongMapNode[] values = this.values;
        if (values == null) {
            return;
        }
        int len = values.length;
        for (int i = 0; i < len; i++) {
            LongMapNode<V> mn = values[i];
            if (mn == null) {
                continue;
            }
            LongMapNode<V> prev = null;
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

    private boolean visit(Visitor<V> visitor, ReadOnlyVisitor<V> readOnlyVisitor, long key, V value) {
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
        boolean visit(long key, V value);
    }

    @FunctionalInterface
    public interface ReadOnlyVisitor<V> {
        void visit(long key, V value);
    }

    public static <V> Pair<V, LongObjMap<V>> copyOnWritePut(LongObjMap<V> map, long key, V value) {
        LongObjMap<V> newMap = copyMap(map);
        V oldValue = newMap.put(key, value);
        return new Pair<>(oldValue, newMap);
    }

    public static <V> Pair<V, LongObjMap<V>> copyOnWriteRemove(LongObjMap<V> map, long key) {
        LongObjMap<V> newMap = copyMap(map);
        V oldValue = newMap.remove(key);
        return new Pair<>(oldValue, newMap);
    }

    private static <V> LongObjMap<V> copyMap(LongObjMap<V> map) {
        LongObjMap<V> newMap;
        LongMapNode[] values = map.values;
        if (values == null) {
            newMap = new LongObjMap<>(map.resizeThreshold, map.loadFactor);
        } else {
            if (map.size + 1 >= map.resizeThreshold && values.length < MAX_ARRAY_SIZE) {
                newMap = new LongObjMap<>(values.length << 1, map.loadFactor);
            } else {
                newMap = new LongObjMap<>(values.length, map.loadFactor);
            }
        }
        map.forEach(newMap::put);
        return newMap;
    }
}
