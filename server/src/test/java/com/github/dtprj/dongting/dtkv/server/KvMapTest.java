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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.common.ByteArray;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KvMap implementation (HashMap + doubly-linked list).
 *
 * @author huangli
 */
class KvMapTest {

    private static ByteArray ba(String str) {
        return new ByteArray(str.getBytes());
    }

    private KvNodeHolder createHolder(ByteArray key) {
        KvNodeEx node = new KvNodeEx(1, 100, 1, 100, 0, "value".getBytes());
        return new KvNodeHolder(key, key, node, null);
    }

    @Test
    void testPutAndGet() {
        KvMap map = new KvMap(16, 0.75f);

        ByteArray key1 = ba("key1");
        KvNodeHolder holder1 = createHolder(key1);
        map.put(key1, holder1);

        assertSame(holder1, map.get(key1));
        assertEquals(1, map.size());
    }

    @Test
    void testPutReplacesExisting() {
        KvMap map = new KvMap(16, 0.75f);

        ByteArray key1 = ba("key1");
        KvNodeHolder holder1 = createHolder(key1);
        KvNodeHolder holder2 = createHolder(key1);

        map.put(key1, holder1);
        KvNodeHolder old = map.put(key1, holder2);

        assertSame(holder1, old);
        assertSame(holder2, map.get(key1));
        assertEquals(1, map.size());
    }

    @Test
    void testRemove() {
        KvMap map = new KvMap(16, 0.75f);

        ByteArray key1 = ba("key1");
        KvNodeHolder holder1 = createHolder(key1);
        map.put(key1, holder1);
        assertEquals(1, map.size());

        KvNodeHolder removed = map.remove(key1);
        assertSame(holder1, removed);
        assertNull(map.get(key1));
        assertEquals(0, map.size());
    }

    @Test
    void testRemoveNonExistent() {
        KvMap map = new KvMap(16, 0.75f);

        KvNodeHolder removed = map.remove(ba("nonexistent"));
        assertNull(removed);
        assertEquals(0, map.size());
    }

    @Test
    void testEmptyMap() {
        KvMap map = new KvMap(16, 0.75f);

        assertNull(map.get(ba("any")));
        assertEquals(0, map.size());
        assertFalse(map.iterator().hasNext());
    }

    @Test
    void testIteratorBasic() {
        KvMap map = new KvMap(16, 0.75f);

        ByteArray key1 = ba("key1");
        ByteArray key2 = ba("key2");
        ByteArray key3 = ba("key3");

        map.put(key1, createHolder(key1));
        map.put(key2, createHolder(key2));
        map.put(key3, createHolder(key3));

        Iterator<KvNodeHolder> it = map.iterator();
        int count = 0;
        while (it.hasNext()) {
            KvNodeHolder h = it.next();
            assertNotNull(h);
            assertNotNull(h.key);
            count++;
        }
        assertEquals(3, count);
    }

    @Test
    void testIteratorNoRemove() {
        KvMap map = new KvMap(16, 0.75f);
        map.put(ba("key1"), createHolder(ba("key1")));

        Iterator<KvNodeHolder> it = map.iterator();
        assertTrue(it.hasNext());
        it.next();

        assertThrows(UnsupportedOperationException.class, it::remove);
    }

    @Test
    void testIteratorEmptyThrows() {
        KvMap map = new KvMap(16, 0.75f);

        Iterator<KvNodeHolder> it = map.iterator();
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    void testMultiplePutsAndRemoves() {
        KvMap map = new KvMap(16, 0.75f);

        for (int i = 0; i < 100; i++) {
            ByteArray key = ba("key" + i);
            map.put(key, createHolder(key));
        }
        assertEquals(100, map.size());

        for (int i = 0; i < 50; i++) {
            ByteArray key = ba("key" + i);
            map.remove(key);
        }
        assertEquals(50, map.size());

        for (int i = 100; i < 150; i++) {
            ByteArray key = ba("key" + i);
            map.put(key, createHolder(key));
        }
        assertEquals(100, map.size());

        for (int i = 0; i < 50; i++) {
            assertNull(map.get(ba("key" + i)));
        }
        for (int i = 50; i < 100; i++) {
            assertNotNull(map.get(ba("key" + i)));
        }
        for (int i = 100; i < 150; i++) {
            assertNotNull(map.get(ba("key" + i)));
        }
    }

    @Test
    void testIteratorConcurrentModification() {
        KvMap map = new KvMap(16, 0.75f);

        map.put(ba("key1"), createHolder(ba("key1")));
        map.put(ba("key2"), createHolder(ba("key2")));
        map.put(ba("key3"), createHolder(ba("key3")));

        Iterator<KvNodeHolder> it = map.iterator();
        int count = 0;
        while (it.hasNext()) {
            KvNodeHolder h = it.next();
            count++;

            if (ba("key2").equals(h.key)) {
                map.remove(h.key);
            }
        }
        assertEquals(3, count);
        assertEquals(2, map.size());
    }

    @Test
    void testIteratorWithPutDuringIteration() {
        KvMap map = new KvMap(16, 0.75f);

        map.put(ba("key1"), createHolder(ba("key1")));

        Iterator<KvNodeHolder> it = map.iterator();
        int initialCount = 0;
        while (it.hasNext()) {
            it.next();
            initialCount++;
            map.put(ba("key2"), createHolder(ba("key2")));
        }
        assertEquals(1, initialCount);
        assertEquals(2, map.size());
    }

    @Test
    void testO1RemovalFromList() {
        KvMap map = new KvMap(16, 0.75f);

        KvNodeHolder holder1 = createHolder(ba("key1"));
        KvNodeHolder holder2 = createHolder(ba("key2"));
        KvNodeHolder holder3 = createHolder(ba("key3"));

        map.put(ba("key1"), holder1);
        map.put(ba("key2"), holder2);
        map.put(ba("key3"), holder3);

        map.remove(ba("key2"));

        assertNull(map.get(ba("key2")));
        assertEquals(2, map.size());

        Iterator<KvNodeHolder> it = map.iterator();
        assertTrue(it.hasNext());
        assertSame(holder1, it.next());
        assertTrue(it.hasNext());
        assertSame(holder3, it.next());
        assertFalse(it.hasNext());

        assertNull(holder2.prev);
        assertNull(holder2.next);
    }

    @Test
    void testRemoveHeadElement() {
        KvMap map = new KvMap(16, 0.75f);

        ByteArray key1 = ba("key1");
        KvNodeHolder holder1 = createHolder(key1);
        map.put(key1, holder1);

        map.remove(key1);

        assertEquals(0, map.size());
        assertFalse(map.iterator().hasNext());
        assertNull(holder1.prev);
        assertNull(holder1.next);
    }

    @Test
    void testPutAfterRemove() {
        KvMap map = new KvMap(16, 0.75f);

        ByteArray key1 = ba("key1");
        KvNodeHolder holder1 = createHolder(key1);
        map.put(key1, holder1);

        map.remove(key1);

        KvNodeHolder holder2 = createHolder(key1);
        map.put(key1, holder2);

        assertSame(holder2, map.get(key1));
        assertEquals(1, map.size());
    }

    @Test
    void testIteratorPreservesOrder() {
        KvMap map = new KvMap(16, 0.75f);

        for (int i = 0; i < 10; i++) {
            map.put(ba("key" + i), createHolder(ba("key" + i)));
        }

        Iterator<KvNodeHolder> it = map.iterator();
        for (int i = 0; i < 10; i++) {
            assertTrue(it.hasNext());
            KvNodeHolder h = it.next();
            assertEquals("key" + i, new String(h.key.getData()));
        }
        assertFalse(it.hasNext());
    }
}
