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

public class LongLongSeqMapTest {
    private LongLongSeqMap map;

    @BeforeEach
    public void setUp() {
        map = new LongLongSeqMap(8);
    }

    @Test
    public void testPutAndGet() {
        map.put(0, 100);
        map.put(1, 200);
        map.put(2, 300);

        assertEquals(100, map.get(0));
        assertEquals(200, map.get(1));
        assertEquals(300, map.get(2));
    }

    @Test
    public void testRemove() {
        map.put(0, 100);
        map.put(1, 200);
        map.put(2, 300);
        map.remove();
        map.remove();

        assertThrows(IllegalArgumentException.class, () -> map.get(0));
        assertThrows(IllegalArgumentException.class, () -> map.get(1));
        assertEquals(300, map.get(2));
    }

    @Test
    public void testSize() {
        map.put(0, 100);
        map.put(1, 200);
        map.put(2, 300);

        assertEquals(3, map.size());
        map.remove();
        assertEquals(2, map.size());
    }

    @Test
    public void testGetFirstKey() {
        map.put(0, 100);
        map.put(1, 200);
        map.put(2, 300);

        assertEquals(0, map.getFirstKey());
        map.remove();
        assertEquals(1, map.getFirstKey());
    }

    @Test
    public void testGetLastKey() {
        map.put(0, 100);
        map.put(1, 200);
        map.put(2, 300);

        assertEquals(2, map.getLastKey());
        map.remove();
        assertEquals(2, map.getLastKey());
    }

    @Test
    public void testResize() {
        for (long i = 0; i < 10; i++) {
            map.put(i, i * 100);
        }

        assertEquals(10, map.size());
        for (long i = 0; i < 10; i++) {
            assertEquals(i * 100, map.get(i));
        }
    }

    @Test
    public void testResize2() {
        int base = 15;
        for (long i = 0; i < 8; i++) {
            map.put(base + i, (base + i) * 100);
        }
        assertEquals(8, map.size());
        for (long i = 0; i < 8; i++) {
            long key = map.getLastKey() + 1;
            map.put(key, key * 100);
        }
        assertEquals(16, map.size());
        for (long i = map.getFirstKey(); i <= map.getLastKey(); i++) {
            assertEquals(i * 100, map.get(i));
        }
    }

    @Test
    public void testCircularBuffer() {
        for (long i = 0; i < 8; i++) {
            map.put(i, i * 100);
        }
        map.remove();
        map.remove();
        map.remove();
        map.remove();

        map.put(8, 800);
        map.put(9, 900);
        map.put(10, 1000);
        map.put(11, 1100);

        for (int i = 4; i < 12; i++) {
            assertEquals(i * 100, map.get(i));
        }

        for (int i = 12; i < 100; i++) {
            map.put(i, i * 100);
        }

        for (int i = 4; i < 100; i++) {
            assertEquals(i * 100, map.get(i));
        }
    }

    @Test
    public void testIllegalArgument() {
        map.put(0, 100);
        map.put(1, 200);

        assertThrows(IllegalArgumentException.class, () -> map.put(3, 400));
    }

    @Test
    public void testTruncate() {
        for (long i = 0; i < 10; i++) {
            map.put(i, i * 100);
        }

        map.truncate(6);
        assertEquals(6, map.size());
        assertEquals(0, map.getFirstKey());
        assertEquals(5, map.getLastKey());

        for (long i = 0; i <= 5; i++) {
            assertEquals(i * 100, map.get(i));
        }
    }

    @Test
    public void testTruncate2() {
        for (long i = 0; i < 8; i++) {
            map.put(i, i * 100);
        }
        map.remove();
        map.remove();
        map.put(8, 800);
        map.put(9, 900);

        map.truncate(4);
        assertEquals(2, map.size());
        assertEquals(2, map.getFirstKey());
        assertEquals(3, map.getLastKey());

        assertEquals(200, map.get(2));
        assertEquals(300, map.get(3));
    }

    @Test
    public void testTruncateWithInvalidKey() {
        for (long i = 0; i < 10; i++) {
            map.put(i, i * 100);
        }

        assertThrows(IllegalArgumentException.class, () -> map.truncate(10));
    }

    @Test
    public void testTruncateAtFirstKey() {
        for (long i = 0; i < 10; i++) {
            map.put(i, i * 100);
        }

        map.truncate(0);
        assertEquals(0, map.size());
        assertEquals(-1, map.getFirstKey());
        assertEquals(-1, map.getLastKey());
    }

    @Test
    public void testTruncateAtLastKey() {
        for (long i = 0; i < 10; i++) {
            map.put(i, i * 100);
        }

        map.truncate(9);
        assertEquals(9, map.size());
        assertEquals(0, map.getFirstKey());
        assertEquals(8, map.getLastKey());
    }
}
