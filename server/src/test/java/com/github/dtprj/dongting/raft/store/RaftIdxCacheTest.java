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
        map.put(1, 0, 1000);
        map.put(2, 1000, 200);
        map.put(3, 1200, 300);

        assertEquals(0, map.get(1));
        assertEquals(1000, map.lastGetSize);
        assertEquals(1000, map.get(2));
        assertEquals(200, map.lastGetSize);
        assertEquals(1200, map.get(3));
        assertEquals(300, map.lastGetSize);
    }

    @Test
    public void testRemove() {
        map.put(1, 0, 100);
        map.put(2, 100, 200);
        map.put(3, 300, 500);
        map.remove();
        map.remove();

        assertThrows(IllegalArgumentException.class, () -> map.get(0));
        assertThrows(IllegalArgumentException.class, () -> map.get(1));
        assertEquals(300, map.get(3));
        assertEquals(500, map.lastGetSize);
    }

    @Test
    public void testSize() {
        map.put(1, 0, 100);
        map.put(2, 100, 200);
        map.put(3, 300, 1000);

        assertEquals(3, map.size());
        map.remove();
        assertEquals(2, map.size());
    }

    @Test
    public void testGetFirstKey() {
        map.put(1, 0, 100);
        map.put(2, 100, 200);
        map.put(3, 300, 800);

        assertEquals(1, map.getFirstRaftIndex());
        map.remove();
        assertEquals(2, map.getFirstRaftIndex());
    }

    @Test
    public void testGetLastKey() {
        map.put(1, 0, 100);
        map.put(2, 100, 200);
        map.put(3, 300, 400);

        assertEquals(3, map.getLastRaftIndex());
        map.remove();
        assertEquals(3, map.getLastRaftIndex());
    }

    @Test
    public void testResize() {
        for (long i = 0; i < 10; i++) {
            int raftIndex = (int) (i + 1);
            map.put(raftIndex, raftIndex * 100, 100);
        }

        assertEquals(10, map.size());
        for (long i = 0; i < 10; i++) {
            int raftIndex = (int) (i + 1);
            assertEquals(raftIndex * 100, map.get(raftIndex));
        }
    }

    @Test
    public void testResize2() {
        int base = 15;
        for (long i = 0; i < 8; i++) {
            map.put(base + i, (base + i) * 100, 100);
        }
        assertEquals(8, map.size());
        for (long i = 0; i < 8; i++) {
            long key = map.getLastRaftIndex() + 1;
            map.put(key, key * 100, 100);
        }
        assertEquals(16, map.size());
        for (long i = map.getFirstRaftIndex(); i <= map.getLastRaftIndex(); i++) {
            assertEquals(i * 100, map.get(i));
        }
    }

    @Test
    public void testCircularBuffer() {
        for (long i = 0; i < 8; i++) {
            map.put(i, i * 100, 100);
        }
        map.remove();
        map.remove();
        map.remove();
        map.remove();

        map.put(8, 800, 100);
        map.put(9, 900, 100);
        map.put(10, 1000, 100);
        map.put(11, 1100, 100);

        for (int i = 4; i < 12; i++) {
            assertEquals(i * 100, map.get(i));
        }

        for (int i = 12; i < 100; i++) {
            map.put(i, i * 100, 100);
        }

        for (int i = 4; i < 100; i++) {
            assertEquals(i * 100, map.get(i));
        }
    }

    @Test
    public void testIllegalArgument() {
        map.put(1, 0, 100);
        map.put(2, 100, 200);

        assertThrows(IllegalArgumentException.class, () -> map.put(300, 400, 400));
    }

    @Test
    public void testTruncate() {
        for (long i = 0; i < 10; i++) {
            map.put(i, i * 100, 100);
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
            map.put(i, i * 100, 100);
        }
        map.remove();
        map.remove();
        map.put(8, 800, 100);
        map.put(9, 900, 100);

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
            map.put(i, i * 100, 100);
        }

        assertThrows(IllegalArgumentException.class, () -> map.truncate(10));
    }

    @Test
    public void testTruncateAtFirstKey() {
        for (long i = 0; i < 10; i++) {
            map.put(i, i * 100, 100);
        }

        map.truncate(0);
        assertEquals(0, map.size());
        assertEquals(-1, map.getFirstRaftIndex());
        assertEquals(-1, map.getLastRaftIndex());
    }

    @Test
    public void testTruncateAtLastKey() {
        for (long i = 0; i < 10; i++) {
            map.put(i, i * 100, 100);
        }

        map.truncate(9);
        assertEquals(9, map.size());
        assertEquals(0, map.getFirstRaftIndex());
        assertEquals(8, map.getLastRaftIndex());
    }
}
