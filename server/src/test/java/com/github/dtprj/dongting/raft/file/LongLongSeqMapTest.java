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
        map.remove(2);

        assertEquals(0, map.get(0));
        assertEquals(0, map.get(1));
        assertEquals(300, map.get(2));
    }

    @Test
    public void testSize() {
        map.put(0, 100);
        map.put(1, 200);
        map.put(2, 300);

        assertEquals(3, map.size());
        map.remove(1);
        assertEquals(2, map.size());
    }

    @Test
    public void testGetFirstKey() {
        map.put(0, 100);
        map.put(1, 200);
        map.put(2, 300);

        assertEquals(0, map.getFirstKey());
        map.remove(1);
        assertEquals(1, map.getFirstKey());
    }

    @Test
    public void testGetLastKey() {
        map.put(0, 100);
        map.put(1, 200);
        map.put(2, 300);

        assertEquals(2, map.getLastKey());
        map.remove(1);
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
    public void testCircularBuffer() {
        for (long i = 0; i < 8; i++) {
            map.put(i, i * 100);
        }
        map.remove(4);

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
    public void testRemoveAll() {
        map.put(0, 100);
        map.put(1, 200);
        map.remove(10000);
        map.put(5, 500);
        assertEquals(500, map.get(5));
        map.remove(10000);
        for (int i = 0; i < 8; i++) {
            map.put(i, i * 100);
        }
        for (int i = 0; i < 8; i++) {
            assertEquals(i * 100, map.get(i));
        }
        map.remove(2);
        map.put(8, 800);
        map.put(9, 900);
        map.remove(10000);
        for (int i = 0; i < 8; i++) {
            map.put(i, i * 100);
        }
        for (int i = 0; i < 8; i++) {
            assertEquals(i * 100, map.get(i));
        }
    }

    @Test
    public void testIllegalArgument() {
        map.put(0, 100);
        map.put(1, 200);

        assertThrows(IllegalArgumentException.class, () -> map.put(3, 400));
    }
}
