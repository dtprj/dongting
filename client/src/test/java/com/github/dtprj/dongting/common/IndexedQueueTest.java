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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author huangli
 */
public class IndexedQueueTest {
    private IndexedQueue<Integer> deque;

    @BeforeEach
    public void setUp() {
        deque = new IndexedQueue<>(16);
    }

    @Test
    public void testAddLastAndSize() {
        deque.addLast(1);
        deque.addLast(2);
        deque.addLast(3);

        assertEquals(3, deque.size());
    }

    @Test
    public void testRemoveFirst() {
        deque.addLast(1);
        deque.addLast(2);
        deque.addLast(3);

        assertEquals(Integer.valueOf(1), deque.removeFirst());
        assertEquals(Integer.valueOf(2), deque.removeFirst());
        assertEquals(Integer.valueOf(3), deque.removeFirst());

        assertNull(deque.removeFirst());
    }

    @Test
    public void testRemoveLast() {
        deque.addLast(1);
        deque.addLast(2);
        assertEquals(Integer.valueOf(2), deque.removeLast());
        assertEquals(Integer.valueOf(1), deque.get(0));
        assertEquals(Integer.valueOf(1), deque.removeLast());
        assertNull(deque.removeLast());
        assertEquals(0, deque.size());
    }

    @Test
    public void testRemoveLast2() {
        deque.addFirst(1);
        deque.addFirst(2);
        assertEquals(Integer.valueOf(1), deque.removeLast());
        assertEquals(Integer.valueOf(2), deque.get(0));
        assertEquals(Integer.valueOf(2), deque.removeLast());
        deque.removeLast();
        assertNull(deque.removeLast());
        assertEquals(0, deque.size());
    }

    @Test
    public void addAddFirst() {
        deque.addLast(1);
        deque.addLast(2);
        deque.addLast(3);
        deque.removeFirst();
        deque.addFirst(1);
        assertEquals(3, deque.size());
        assertEquals(Integer.valueOf(1), deque.get(0));
    }

    @Test
    public void testGet() {
        deque.addLast(1);
        deque.addLast(2);
        deque.addLast(3);

        assertEquals(Integer.valueOf(1), deque.get(0));
        assertEquals(Integer.valueOf(2), deque.get(1));
        assertEquals(Integer.valueOf(3), deque.get(2));
    }

    @Test
    public void testResize() {
        for (int i = 0; i < 20; i++) {
            deque.addLast(i);
        }

        assertEquals(20, deque.size());

        for (int i = 0; i < 20; i++) {
            assertEquals(Integer.valueOf(i), deque.get(i));
        }
    }

    @Test
    public void testWriteIndexLessThanReadIndex() {
        for (int i = 0; i < 10; i++) {
            deque.addLast(i);
        }

        for (int i = 0; i < 5; i++) {
            deque.removeFirst();
        }

        for (int i = 10; i < 20; i++) {
            deque.addLast(i);
        }

        assertEquals(15, deque.size());

        for (int i = 0; i < 15; i++) {
            assertEquals(Integer.valueOf(i + 5), deque.get(i));
        }
    }

    @Test
    public void testResizeWithMovedReadIndex() {
        for (int i = 0; i < 10; i++) {
            deque.addLast(i);
        }

        for (int i = 0; i < 5; i++) {
            deque.removeFirst();
        }

        for (int i = 10; i < 30; i++) {
            deque.addLast(i);
        }

        assertEquals(25, deque.size());

        for (int i = 0; i < 25; i++) {
            assertEquals(Integer.valueOf(i + 5), deque.get(i));
        }
    }
}

