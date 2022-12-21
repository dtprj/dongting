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
        for (int i = 0; i < loop; i++) {
            int key = r.nextInt();
            while (keys.contains(key)) {
                key = r.nextInt();
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
}
