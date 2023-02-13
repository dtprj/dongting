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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author huangli
 */
public class IntObjMapTest {
    @Test
    public void simpleTest() {
        IntObjMap<String> m = new IntObjMap<>();
        assertNull(m.get(1));
        assertNull(m.remove(1));
        assertNull(m.put(1, "123"));
        assertEquals(1, m.size());
        assertEquals("123", m.get(1));
        assertNull(m.get(2));
        assertEquals("123", m.put(1, "123"));

        MutableInt count = new MutableInt(0);
        m.forEach((k, v) -> {
            assertEquals(1L, k);
            assertEquals("123", v);
            count.increment();
            return true;
        });
        assertEquals(1, count.getValue());
        assertEquals("123", m.remove(1));
        assertEquals(0, m.size());
    }

    @Test
    public void fullTest() {
        int loop = 1000;
        IntObjMap<String> m = new IntObjMap<>(7, 0.9f);
        Set<Integer> keys = new HashSet<>();
        Random r = new Random();
        boolean hasZero = false;
        for (int i = 0; i < loop; i++) {
            int key;
            if (!hasZero) {
                key = 0;
                hasZero = true;
            } else {
                key = r.nextInt();
                while (keys.contains(key)) {
                    key = r.nextInt();
                }
            }
            keys.add(key);
            assertNull(m.put(key, String.valueOf(key * 2)));
        }
        assertEquals(loop, m.size());
        for (int key : keys) {
            String v = String.valueOf(key * 2);
            assertEquals(v, m.get(key));
            assertEquals(v, m.put(key, v));
        }

        MutableInt count = new MutableInt(0);
        m.forEach((k, v) -> {
            assertEquals(Integer.parseInt(v), k * 2);
            count.increment();
            return true;
        });
        assertEquals(loop, count.getValue());

        for (int i = 0; i < loop; i++) {
            int key = r.nextInt();
            while (keys.contains(key)) {
                key = r.nextInt();
            }
            assertNull(m.get(key));
        }

        for (int key : keys) {
            String v = String.valueOf(key * 2);
            assertEquals(v, m.remove(key));
        }
        assertEquals(0, m.size());
    }

    private IntObjMap<String> setupForEach(int[] keys) {
        IntObjMap<String> m = new IntObjMap<>(16, 0.99f);
        for (int key : keys) {
            m.put(key, String.valueOf(key));
        }
        return m;
    }

    @Test
    public void testForEach() {
        int[] keys = new int[]{1, 17, 33, 2, 18, 3};

        IntObjMap<String> m = setupForEach(keys);
        int size = m.size();
        m.forEach((k, v) -> false);
        assertEquals(0, m.size());

        for (int key : keys) {
            m = setupForEach(keys);
            // remove the key from map though forEach
            m.forEach((k, v) -> k != key);
            assertEquals(size - 1, m.size());
            assertNull(m.get(key));
            for (int k : keys) {
                if (k != key) {
                    assertEquals(String.valueOf(k), m.get(k));
                }
            }

            // re-put
            m.put(key, String.valueOf(key));
            for (int k : keys) {
                assertEquals(String.valueOf(k), m.get(k));
            }
            assertEquals(size, m.size());
        }
    }

    @Test
    public void testInVisitStatus() {
        IntObjMap<String> m = new IntObjMap<>();
        m.put(1, "1");
        assertThrows(IllegalStateException.class, () -> m.forEach((k, v) -> {
            m.forEach((k2, v2) -> true);
            return true;
        }));
        assertThrows(IllegalStateException.class, () -> m.forEach((k, v) -> {
            m.put(2, "2");
            return true;
        }));
        assertThrows(IllegalStateException.class, () -> m.forEach((k, v) -> {
            m.remove(3);
            return true;
        }));
    }
}
