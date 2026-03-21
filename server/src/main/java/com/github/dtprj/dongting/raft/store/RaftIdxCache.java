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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.common.DtUtil;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class RaftIdxCache {

    private static final int SLOT_SIZE = 12;
    private int capacity;
    private byte[] data;
    private ByteBuffer buffer;
    private long firstRaftIndex;
    private long lastRaftIndex;
    private int size;

    public RaftIdxCache(int initialCapacity) {
        int capacity = 1;
        while (capacity < initialCapacity) {
            capacity <<= 1;
        }
        data = new byte[capacity * SLOT_SIZE];
        buffer = ByteBuffer.wrap(data);
        this.capacity = capacity;
        firstRaftIndex = -1;
        lastRaftIndex = -1;
    }

    public void put(long raftIndex, long pos, int itemSize) {
        if (lastRaftIndex != -1 && raftIndex != lastRaftIndex + 1) {
            throw new IllegalArgumentException("raftIndex not continuous");
        }
        if (size >= capacity) {
            resize();
        }
        if (firstRaftIndex == -1) {
            firstRaftIndex = raftIndex;
        }
        lastRaftIndex = raftIndex;
        int index = bufIndex(raftIndex, capacity);
        buffer.putLong(index, pos);
        buffer.putInt(index + 8, itemSize);
        size++;
    }

    int lastGetSize;

    public long get(long raftIndex) {
        if (raftIndex >= firstRaftIndex && raftIndex <= lastRaftIndex) {
            int slotIndex = bufIndex(raftIndex, capacity);
            lastGetSize = buffer.getInt(slotIndex + 8);
            return buffer.getLong(slotIndex);
        }
        throw new IllegalArgumentException("bad raftIndex " + raftIndex + ",first=" + firstRaftIndex + ",last=" + lastRaftIndex);
    }

    public void remove() {
        if (size > 0) {
            int index = bufIndex(firstRaftIndex, capacity);
            buffer.putLong(index, 0);
            buffer.putInt(index + 8, 0);
            firstRaftIndex++;
            size--;
            if (size == 0) {
                firstRaftIndex = -1;
                lastRaftIndex = -1;
            }
        }
    }

    public int size() {
        return size;
    }

    public long getFirstRaftIndex() {
        return firstRaftIndex;
    }

    public long getLastRaftIndex() {
        return lastRaftIndex;
    }

    private int bufIndex(long raftIndex, int len) {
        return (((int) raftIndex) & (len - 1)) * SLOT_SIZE;
    }

    private void resize() {
        int oldCapacity = this.capacity;
        int newCapacity = oldCapacity << 1;
        byte[] newData = new byte[newCapacity * SLOT_SIZE];

        int firstPartItems = (data.length - bufIndex(firstRaftIndex, oldCapacity)) / SLOT_SIZE;
        int secondPartItems = oldCapacity - firstPartItems;

        System.arraycopy(data, bufIndex(firstRaftIndex, oldCapacity), newData,
                bufIndex(firstRaftIndex, newCapacity), firstPartItems * SLOT_SIZE);
        if (secondPartItems > 0) {
            System.arraycopy(data, 0, newData, bufIndex((firstRaftIndex + firstPartItems), newCapacity),
                    secondPartItems * SLOT_SIZE);
        }

        this.data = newData;
        this.buffer = ByteBuffer.wrap(newData);
        this.capacity = newCapacity;
    }

    /**
     * truncate tail to the given raftIndex (inclusive)
     */
    public void truncate(long raftIndex) {
        if (raftIndex < firstRaftIndex || raftIndex > lastRaftIndex) {
            throw new IllegalArgumentException("Invalid raftIndex to truncate");
        }
        while (lastRaftIndex >= raftIndex) {
            int bufIndex = bufIndex(lastRaftIndex, capacity);
            buffer.putLong(bufIndex, 0);
            buffer.putInt(bufIndex + 8, 0);
            lastRaftIndex--;
            size--;
        }
        if (size == 0) {
            firstRaftIndex = -1;
            lastRaftIndex = -1;
        }
    }

    public long fill(long index, ByteBuffer destBuffer) {
        DtUtil.checkPositive(destBuffer.remaining(), "destBuffer.remaining");
        if (destBuffer.remaining() % IdxFileQueue.ITEM_LEN != 0) {
            throw new IllegalArgumentException("invalid buf size " + destBuffer.remaining());
        }
        int count = destBuffer.remaining() / IdxFileQueue.ITEM_LEN;
        if (index < firstRaftIndex || index > lastRaftIndex) {
            throw new IllegalArgumentException("bad index " + index + ", firstRaftIndex="
                    + firstRaftIndex + ", lastRaftIndex=" + lastRaftIndex);
        }
        if ((index + count - 1) < firstRaftIndex || (index + count - 1) > lastRaftIndex) {
            throw new IllegalArgumentException("bad index " + index + "," + destBuffer.remaining()
                    + ", firstRaftIndex=" + firstRaftIndex + ", lastRaftIndex=" + lastRaftIndex);
        }
        // the crc32c is a simple object, so here no need to cache
        CRC32C crc = new CRC32C();
        byte[] data = this.data;
        for (int i = 0; i < count; i++, index++) {
            int srcBufferIndex = bufIndex(index, capacity);
            crc.update(data, srcBufferIndex, SLOT_SIZE);
            destBuffer.put(data, srcBufferIndex, SLOT_SIZE);
            destBuffer.putInt((int) crc.getValue());
            crc.reset();
        }
        destBuffer.flip();
        return index;
    }
}

