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
package com.github.dtprj.dongting.bench.map;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author huangli
 */
public class OrderMapTest {
    private static final int KEYS = 100_000;
    private static final int LOOP = 10;

    public static void main(String[] args) {
        List<Integer> values = new ArrayList<>(KEYS);
        for (int i = 0; i < KEYS; i++) {
            values.add(i);
        }
        Collections.shuffle(values);
        System.out.println("HashMap test:");
        test(new HashMap<>(), values, false);
        test(new HashMap<>(), values, true);

        System.out.println("\nConcurrentHashMap test:");
        test(new ConcurrentHashMap<>(), values, false);
        test(new ConcurrentHashMap<>(), values, true);

        System.out.println("\nTreeMap test:");
        test(new TreeMap<>(), values, false);
        test(new TreeMap<>(), values, true);

        System.out.println("\nConcurrentSkipListMap test:");
        test(new ConcurrentSkipListMap<>(), values, false);
        test(new ConcurrentSkipListMap<>(), values, true);
    }

    private static void test(Map<Integer, Integer> m, List<Integer> values, boolean print) {
        long t = System.currentTimeMillis();
        for (int i = 0; i < KEYS; i++) {
            m.put(i, i);
        }
        if (print) {
            System.out.println("build cost: " + (System.currentTimeMillis() - t) + "ms");
        }
        t = System.currentTimeMillis();
        for (int loop = 0; loop < LOOP; loop++) {
            for (int i = 0; i < KEYS; i++) {
                Integer v = values.get(i);
                m.put(v, v);
            }
        }
        if (print) {
            System.out.println("put cost: " + (System.currentTimeMillis() - t) + "ms");
        }
    }
}
