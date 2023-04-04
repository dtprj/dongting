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
package com.github.dtprj.dongting.raft.file;

import com.github.dtprj.dongting.common.ObjUtil;

/**
 * @author huangli
 */
public class LongLongSeqMap {
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private long[] data;
    private long firstKey;
    private long lastKey;
    private int size;

    public LongLongSeqMap() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public LongLongSeqMap(int initialCapacity) {
        int capacity = 1;
        while (capacity < initialCapacity) {
            capacity <<= 1;
        }
        data = new long[capacity];
        firstKey = -1;
        lastKey = -1;
    }

    public void put(long key, long value) {
        if (lastKey != -1 && key != lastKey + 1) {
            throw new IllegalArgumentException("Key must be exactly 1 greater than the last key");
        }
        if (size >= data.length) {
            resize();
        }
        if (firstKey == -1) {
            firstKey = key;
        }
        lastKey = key;
        data[idx(key, data.length)] = value;
        size++;
    }

    public long get(long key) {
        if (key >= firstKey && key <= lastKey) {
            return data[idx(key, data.length)];
        }
        return 0;
    }

    public void remove(int count) {
        ObjUtil.checkPositive(count, "count");
        for (int i = 0; i < count && size > 0; i++) {
            data[idx(firstKey, data.length)] = 0;
            firstKey++;
            size--;
        }
        if (size == 0) {
            firstKey = -1;
            lastKey = -1;
        }
    }

    public int size() {
        return size;
    }

    public long getFirstKey() {
        return firstKey;
    }

    public long getLastKey() {
        return lastKey;
    }

    private int idx(long key, int len) {
        return (int) (key & (len - 1));
    }

    private void resize() {
        int oldCapacity = data.length;
        int newCapacity = oldCapacity << 1;
        long[] newData = new long[newCapacity];

        int firstPartLength = oldCapacity - idx(firstKey, oldCapacity);
        int secondPartLength = oldCapacity - firstPartLength;

        System.arraycopy(data, idx(firstKey, oldCapacity), newData, idx(firstKey, newCapacity), firstPartLength);
        if (secondPartLength > 0) {
            System.arraycopy(data, 0, newData, idx(firstKey, newCapacity) + firstPartLength, secondPartLength);
        }

        data = newData;
    }

    public void truncate(long key) {
        if (key < firstKey || key > lastKey) {
            throw new IllegalArgumentException("Invalid key to truncate");
        }
        while (lastKey >= key) {
            data[idx(lastKey, data.length)] = 0;
            lastKey--;
            size--;
        }
        if (size == 0) {
            firstKey = -1;
            lastKey = -1;
        }
    }
}

