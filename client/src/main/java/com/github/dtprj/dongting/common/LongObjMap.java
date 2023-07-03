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

import com.github.dtprj.dongting.log.BugLog;

import java.util.Objects;

/**
 * This class is not thread safe.
 * @author huangli
 */
public class LongObjMap<V> {
    private static final int MAX_ARRAY_SIZE = 1 << 30;
    private int size;
    private int resizeThreshold;
    private final float loadFactor;
    private long[] keys;
    private Object[] values;
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

    @SuppressWarnings("unchecked")
    private V find(long key, boolean remove) {
        int h = hashCode(key);
        Object[] values = this.values;
        if (values == null) {
            return null;
        }
        int idx = h & (values.length - 1);
        Object existData = values[idx];
        if (existData == null) {
            return null;
        }
        if (existData instanceof LongMapNode) {
            LongMapNode<V> mn = (LongMapNode<V>) existData;
            if (mn.getKey() == key) {
                if (remove) {
                    LongMapNode<V> next = mn.getNext();
                    values[idx] = next;
                    if (next != null) {
                        keys[idx] = next.getKey();
                    } else {
                        keys[idx] = 0L;
                    }
                    size--;
                }
                return mn.getValue();
            }
            LongMapNode<V> next;
            while ((next = mn.getNext()) != null) {
                if (next.getKey() == key) {
                    if (remove) {
                        mn.setNext(next.getNext());
                        size--;
                    }
                    return next.getValue();
                }
                mn = next;
            }
            return null;
        } else {
            long[] keys = this.keys;
            if (keys[idx] == key) {
                if (remove) {
                    size--;
                    keys[idx] = 0L;
                    values[idx] = null;
                }
                return (V) existData;
            } else {
                return null;
            }
        }
    }

    public V put(long key, V value) {
        Objects.requireNonNull(value);
        if (readVisit || rwVisit) {
            throw new IllegalStateException("can modify the map during iteration");
        }
        long[] keys = resize();
        Object[] values = this.values;
        V r = put0(keys, values, key, value, keys.length - 1);
        if (r == null) {
            size++;
        }
        return r;
    }

    @SuppressWarnings("unchecked")
    private V put0(long[] keys, Object[] values, long key, V value, int mask) {
        int h = hashCode(key);
        int idx = h & mask;
        Object existData = values[idx];
        if (existData == null) {
            keys[idx] = key;
            values[idx] = value;
            return null;
        } else {
            LongMapNode<V> mn;
            if (existData instanceof LongMapNode) {
                mn = (LongMapNode<V>) existData;
                while (true) {
                    if (mn.getKey() == key) {
                        V old = mn.getValue();
                        mn.setValue(value);
                        return old;
                    }
                    LongMapNode<V> next = mn.getNext();
                    if (next == null) {
                        LongMapNode<V> newNode = new LongMapNode<>(key, value);
                        mn.setNext(newNode);
                        return null;
                    } else {
                        mn = next;
                    }
                }
            } else {
                if (keys[idx] == key) {
                    values[idx] = value;
                    return (V) existData;
                } else {
                    mn = new LongMapNode<>(keys[idx], (V) existData);
                    values[idx] = mn;
                    LongMapNode<V> newNode = new LongMapNode<>(key, value);
                    mn.setNext(newNode);
                    return null;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private long[] resize() {
        int threshold = this.resizeThreshold;
        long[] oldKeys = this.keys;
        if (oldKeys == null) {
            keys = new long[threshold];
            values = new Object[threshold];
            this.resizeThreshold = (int) (loadFactor * threshold);
            return keys;
        }
        if (size < threshold) {
            return oldKeys;
        }

        int oldArrayLength = oldKeys.length;
        if (oldArrayLength >= MAX_ARRAY_SIZE) {
            return oldKeys;
        }
        int newSize = oldKeys.length << 1;
        this.resizeThreshold = (int) (loadFactor * newSize);
        int mask = newSize - 1;
        long[] newKeys = new long[newSize];
        Object[] newValues = new Object[newSize];
        Object[] values = this.values;
        int len = values.length;
        for (int i = 0; i < len; i++) {
            Object v = values[i];
            if (v == null) {
                continue;
            }
            if (v instanceof LongMapNode) {
                LongMapNode<V> mn = (LongMapNode<V>) v;
                do {
                    put0(newKeys, newValues, mn.getKey(), mn.getValue(), mask);
                    mn = mn.getNext();
                } while (mn != null);
            } else {
                put0(newKeys, newValues, keys[i], (V) v, mask);
            }
        }
        this.keys = newKeys;
        this.values = newValues;
        return newKeys;
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
        long[] keys = this.keys;
        if (keys == null) {
            return;
        }
        Object[] values = this.values;
        int len = values.length;
        for (int i = 0; i < len; i++) {
            Object v = values[i];
            if (v == null) {
                continue;
            }
            if (v instanceof LongMapNode) {
                LongMapNode<V> prev = null;
                boolean first = true;
                @SuppressWarnings("unchecked")
                LongMapNode<V> mn = (LongMapNode<V>) v;
                do {
                    boolean keep = visit(visitor, readOnlyVisitor, mn.getKey(), mn.getValue());
                    if (!keep) {
                        if (first) {
                            LongMapNode<V> next = mn.getNext();
                            if (next == null) {
                                BugLog.getLog().error("LongObjMap: next is null");
                            } else {
                                keys[i] = next.getKey();
                                if (next.getNext() == null) {
                                    values[i] = next.getValue();
                                } else {
                                    values[i] = next;
                                }
                            }
                        } else {
                            if (prev != null) {
                                prev.setNext(mn.getNext());
                            } else {
                                keys[i] = mn.getKey();
                                if (mn.getNext() == null) {
                                    values[i] = mn.getValue();
                                } else {
                                    values[i] = mn;
                                }
                            }
                        }
                        size--;
                    } else {
                        prev = mn;
                    }
                    mn = mn.getNext();
                    first = false;
                } while (mn != null);
            } else {
                @SuppressWarnings("unchecked")
                boolean keep = visit(visitor, readOnlyVisitor, keys[i], (V) v);
                if (!keep) {
                    values[i] = null;
                    keys[i] = 0L;
                    size--;
                }
            }
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
        long[] keys = map.keys;
        if (keys == null) {
            newMap = new LongObjMap<>(map.resizeThreshold, map.loadFactor);
        } else {
            if (map.size + 1 >= map.resizeThreshold && keys.length < MAX_ARRAY_SIZE) {
                newMap = new LongObjMap<>(keys.length << 1, map.loadFactor);
            } else {
                newMap = new LongObjMap<>(keys.length, map.loadFactor);
            }
        }
        map.forEach((k, v) -> {
            newMap.put(k, v);
            return true;
        });
        return newMap;
    }
}
