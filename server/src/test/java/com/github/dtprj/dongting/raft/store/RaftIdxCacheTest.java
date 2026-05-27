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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author huangli
 */

public class RaftIdxCacheTest {
    private RaftIdxCache map;

    @BeforeEach
    public void setUp() {
        map = new RaftIdxCache(8);
    }

    @Test
    public void testPutAndGet() {
        map.put(1, 0, 1000L, 1000);
        map.put(2, 1000, 2000L, 200);
        map.put(3, 1200, 3000L, 300);

        assertEquals(0, map.get(1));
        assertEquals(1000L, map.lastGetTimestamp);
        assertEquals(1000, map.lastGetSize);
        assertEquals(1000, map.get(2));
        assertEquals(2000L, map.lastGetTimestamp);
        assertEquals(200, map.lastGetSize);
        assertEquals(1200, map.get(3));
        assertEquals(3000L, map.lastGetTimestamp);
        assertEquals(300, map.lastGetSize);
    }

    @Test
    public void testRemove() {
        map.put(1, 0, 1000L, 100);
        map.put(2, 100, 2000L, 200);
        map.put(3, 300, 3000L, 500);
        map.remove();
        map.remove();

        assertThrows(IllegalArgumentException.class, () -> map.get(0));
        assertThrows(IllegalArgumentException.class, () -> map.get(1));
        assertEquals(300, map.get(3));
        assertEquals(3000L, map.lastGetTimestamp);
        assertEquals(500, map.lastGetSize);
    }

    @Test
    public void testSize() {
        map.put(1, 0, 1000L, 100);
        map.put(2, 100, 2000L, 200);
        map.put(3, 300, 3000L, 1000);

        assertEquals(3, map.size());
        map.remove();
        assertEquals(2, map.size());
    }

    @Test
    public void testGetFirstKey() {
        map.put(1, 0, 1000L, 100);
        map.put(2, 100, 2000L, 200);
        map.put(3, 300, 3000L, 800);

        assertEquals(1, map.getFirstRaftIndex());
        map.remove();
        assertEquals(2, map.getFirstRaftIndex());
    }

    @Test
    public void testGetLastKey() {
        map.put(1, 0, 1000L, 100);
        map.put(2, 100, 2000L, 200);
        map.put(3, 300, 3000L, 400);

        assertEquals(3, map.getLastRaftIndex());
        map.remove();
        assertEquals(3, map.getLastRaftIndex());
    }

    @Test
    public void testResize() {
        for (long i = 0; i < 10; i++) {
            int raftIndex = (int) (i + 1);
            map.put(raftIndex, raftIndex * 100L, raftIndex * 1000L, 100);
        }

        assertEquals(10, map.size());
        for (long i = 0; i < 10; i++) {
            int raftIndex = (int) (i + 1);
            assertEquals(raftIndex * 100L, map.get(raftIndex));
        }
    }

    @Test
    public void testResize2() {
        int base = 15;
        for (long i = 0; i < 8; i++) {
            long index = base + i;
            map.put(index, index * 100, index * 1000L, 100);
        }
        assertEquals(8, map.size());
        for (long i = 0; i < 8; i++) {
            long key = map.getLastRaftIndex() + 1;
            map.put(key, key * 100, key * 1000L, 100);
        }
        assertEquals(16, map.size());
        for (long i = map.getFirstRaftIndex(); i <= map.getLastRaftIndex(); i++) {
            assertEquals(i * 100, map.get(i));
        }
    }

    @Test
    public void testCircularBuffer() {
        for (long i = 0; i < 8; i++) {
            map.put(i, i * 100, i * 1000L, 100);
        }
        map.remove();
        map.remove();
        map.remove();
        map.remove();

        map.put(8, 800, 8000L, 100);
        map.put(9, 900, 9000L, 100);
        map.put(10, 1000, 10000L, 100);
        map.put(11, 1100, 11000L, 100);

        for (int i = 4; i < 12; i++) {
            assertEquals(i * 100L, map.get(i));
        }

        for (int i = 12; i < 100; i++) {
            map.put(i, i * 100L, i * 1000L, 100);
        }

        for (int i = 4; i < 100; i++) {
            assertEquals(i * 100L, map.get(i));
        }
    }

    @Test
    public void testIllegalArgument() {
        map.put(1, 0, 1000L, 100);
        map.put(2, 100, 2000L, 200);

        assertThrows(IllegalArgumentException.class, () -> map.put(300, 400, 0L, 400));
    }

    @Test
    public void testTruncate() {
        for (long i = 0; i < 10; i++) {
            map.put(i, i * 100, i * 1000L, 100);
        }

        map.truncate(6);
        assertEquals(6, map.size());
        assertEquals(0, map.getFirstRaftIndex());
        assertEquals(5, map.getLastRaftIndex());

        for (long i = 0; i <= 5; i++) {
            assertEquals(i * 100, map.get(i));
        }
    }

    @Test
    public void testTruncate2() {
        for (long i = 0; i < 8; i++) {
            map.put(i, i * 100, i * 1000L, 100);
        }
        map.remove();
        map.remove();
        map.put(8, 800, 8000L, 100);
        map.put(9, 900, 9000L, 100);

        map.truncate(4);
        assertEquals(2, map.size());
        assertEquals(2, map.getFirstRaftIndex());
        assertEquals(3, map.getLastRaftIndex());

        assertEquals(200, map.get(2));
        assertEquals(300, map.get(3));
    }

    @Test
    public void testTruncateWithInvalidKey() {
        for (long i = 0; i < 10; i++) {
            map.put(i, i * 100, i * 1000L, 100);
        }

        assertThrows(IllegalArgumentException.class, () -> map.truncate(10));
    }

    @Test
    public void testTruncateAtFirstKey() {
        for (long i = 0; i < 10; i++) {
            map.put(i, i * 100, i * 1000L, 100);
        }

        map.truncate(0);
        assertEquals(0, map.size());
        assertEquals(-1, map.getFirstRaftIndex());
        assertEquals(-1, map.getLastRaftIndex());
    }

    @Test
    public void testTruncateAtLastKey() {
        for (long i = 0; i < 10; i++) {
            map.put(i, i * 100, i * 1000L, 100);
        }

        map.truncate(9);
        assertEquals(9, map.size());
        assertEquals(0, map.getFirstRaftIndex());
        assertEquals(8, map.getLastRaftIndex());
    }

    @Test
    public void testFill() {
        // raftIndex from 1, pos from 0
        map.put(1, 0, 1000L, 100);
        map.put(2, 100, 2000L, 200);
        map.put(3, 300, 3000L, 300);

        // destBuffer.remaining() must be multiple of 32 (IdxFileQueue.ITEM_LEN)
        ByteBuffer buf = ByteBuffer.allocate(96); // 3 items * 32 bytes

        long nextIndex = map.fill(1, buf);
        assertEquals(4, nextIndex); // next index after filling 3 items (1,2,3)

        // verify first item: pos=0, timestamp=1000, size=100
        assertEquals(0, buf.getLong());
        assertEquals(1000L, buf.getLong());
        buf.getLong();
        assertEquals(100, buf.getInt());
        int crc1 = buf.getInt();
        // verify second item: pos=100, timestamp=2000, size=200
        assertEquals(100, buf.getLong());
        assertEquals(2000L, buf.getLong());
        buf.getLong();
        assertEquals(200, buf.getInt());
        int crc2 = buf.getInt();
        // verify third item: pos=300, timestamp=3000, size=300
        assertEquals(300, buf.getLong());
        assertEquals(3000L, buf.getLong());
        buf.getLong();
        assertEquals(300, buf.getInt());
        int crc3 = buf.getInt();

        CRC32C crc = new CRC32C();
        byte[] item1 = new byte[28];
        ByteBuffer.wrap(item1).putLong(0).putLong(1000L).putLong(0L).putInt(100);
        crc.update(item1);
        assertEquals((int) crc.getValue(), crc1);

        byte[] item2 = new byte[28];
        ByteBuffer.wrap(item2).putLong(100).putLong(2000L).putLong(0L).putInt(200);
        crc.reset();
        crc.update(item2);
        assertEquals((int) crc.getValue(), crc2);

        byte[] item3 = new byte[28];
        ByteBuffer.wrap(item3).putLong(300).putLong(3000L).putLong(0L).putInt(300);
        crc.reset();
        crc.update(item3);
        assertEquals((int) crc.getValue(), crc3);
    }

    @Test
    public void testFillPartial() {
        map.put(1, 0, 1000L, 100);
        map.put(2, 100, 2000L, 200);
        map.put(3, 300, 3000L, 300);

        // fill only first 2 items
        ByteBuffer buf = ByteBuffer.allocate(64); // 2 items
        long nextIndex = map.fill(1, buf);
        assertEquals(3, nextIndex);

        assertEquals(0, buf.getLong());
        assertEquals(1000L, buf.getLong());
        buf.getLong();
        assertEquals(100, buf.getInt());
        buf.getInt(); // skip crc
        assertEquals(100, buf.getLong());
        assertEquals(2000L, buf.getLong());
        buf.getLong();
        assertEquals(200, buf.getInt());
    }

    @Test
    public void testFillInvalidBufferSize() {
        map.put(1, 0, 1000L, 100);
        ByteBuffer buf = ByteBuffer.allocate(10); // not multiple of 32
        assertThrows(IllegalArgumentException.class, () -> map.fill(1, buf));
    }

    @Test
    public void testFillInvalidIndex() {
        map.put(1, 0, 1000L, 100);
        ByteBuffer buf = ByteBuffer.allocate(32);
        // index < firstRaftIndex
        assertThrows(IllegalArgumentException.class, () -> map.fill(0, buf));
        // index > lastRaftIndex
        assertThrows(IllegalArgumentException.class, () -> map.fill(2, buf));
    }

    @Test
    public void testFillIndexPlusCountExceed() {
        map.put(1, 0, 1000L, 100);
        map.put(2, 100, 2000L, 200);
        ByteBuffer buf = ByteBuffer.allocate(96); // 3 items, but only 2 in cache
        assertThrows(IllegalArgumentException.class, () -> map.fill(1, buf));
    }

    @Test
    public void testFillEmpty() {
        // fill empty cache - but we need to first add something then remove
        map.put(1, 0, 1000L, 100);
        map.remove();
        // now cache is empty, firstRaftIndex = -1
        ByteBuffer buf = ByteBuffer.allocate(32);
        assertThrows(IllegalArgumentException.class, () -> map.fill(1, buf));
    }
}
