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

/**
 * @author huangli
 */
public class LongObjMapTest {
    @Test
    public void simpleTest() {
        LongObjMap<String> m = new LongObjMap<>();
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
        LongObjMap<String> m = new LongObjMap<>(7, 0.9f);
        Set<Long> keys = new HashSet<>();
        Random r = new Random();
        boolean hasZero = false;
        for (int i = 0; i < loop; i++) {
            long key;
            if (!hasZero) {
                key = 0;
                hasZero = true;
            } else {
                key = r.nextLong();
                while (keys.contains(key)) {
                    key = r.nextLong();
                }
            }
            keys.add(key);
            assertNull(m.put(key, String.valueOf(key * 2)));
        }
        assertEquals(loop, m.size());
        for (long key : keys) {
            String v = String.valueOf(key * 2);
            assertEquals(v, m.get(key));
            assertEquals(v, m.put(key, v));
        }

        MutableInt count = new MutableInt(0);
        m.forEach((k, v) -> {
            assertEquals(Long.parseLong(v), k * 2);
            count.increment();
            return true;
        });
        assertEquals(loop, count.getValue());

        for (int i = 0; i < loop; i++) {
            long key = r.nextLong();
            while (keys.contains(key)) {
                key = r.nextLong();
            }
            assertNull(m.get(key));
        }

        for (long key : keys) {
            String v = String.valueOf(key * 2);
            assertEquals(v, m.remove(key));
        }
        assertEquals(0, m.size());
    }

    private LongObjMap<String> setupForEach(long[] keys) {
        LongObjMap<String> m = new LongObjMap<>(16, 0.99f);
        for (long key : keys) {
            m.put(key, String.valueOf(key));
        }
        return m;
    }

    @Test
    public void testForEach() {
        long[] keys = new long[]{1, 17, 33, 2, 18, 3};

        LongObjMap<String> m = setupForEach(keys);
        int size = m.size();
        m.forEach((k, v) -> false);
        assertEquals(0, m.size());

        for (long key : keys) {
            m = setupForEach(keys);
            // remove the key from map though forEach
            m.forEach((k, v) -> k != key);
            assertEquals(size - 1, m.size());
            assertNull(m.get(key));
            for (long k : keys) {
                if (k != key) {
                    assertEquals(String.valueOf(k), m.get(k));
                }
            }

            // re-put
            m.put(key, String.valueOf(key));
            for (long k : keys) {
                assertEquals(String.valueOf(k), m.get(k));
            }
            assertEquals(size, m.size());
        }
    }
}
