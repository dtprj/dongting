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

import org.opentest4j.AssertionFailedError;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class TestUtil {
    @SuppressWarnings({"unchecked", "unused", "rawtypes"})
    public static void waitUtil(Supplier<Boolean> condition) {
        waitUtil(Boolean.TRUE, (Supplier) condition, 5000);
    }

    @SuppressWarnings("unused")
    public static void waitUtil(Object expectValue, Supplier<Object> actual) {
        waitUtil(expectValue, actual, 5000);
    }

    public static void waitUtil(Object expectValue, Supplier<Object> actual, long timeoutMillis) {
        long start = System.nanoTime();
        long deadline = start + timeoutMillis * 1000 * 1000;
        Object obj = actual.get();
        if (Objects.equals(expectValue, obj)) {
            return;
        }
        int waitCount = 0;
        while (deadline - System.nanoTime() > 0) {
            try {
                //noinspection BusyWait
                Thread.sleep(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitCount++;
            obj = actual.get();
            if(Objects.equals(expectValue, obj)){
                return;
            }
        }
        throw new AssertionFailedError("expect: " + expectValue + ", actual:" + obj + ", timeout="
                + timeoutMillis + "ms, cost=" + (System.nanoTime() - start) / 1000 / 1000 + "ms, waitCount=" + waitCount);
    }
}
