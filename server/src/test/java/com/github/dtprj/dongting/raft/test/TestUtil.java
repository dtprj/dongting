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
package com.github.dtprj.dongting.raft.test;

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.common.Timestamp;
import org.opentest4j.AssertionFailedError;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author huangli
 */
@SuppressWarnings("unused")
public class TestUtil {
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void waitUtil(Supplier<Boolean> condition) {
        waitUtil(Boolean.TRUE, (Supplier) condition, 5000);
    }

    public static void waitUtil(Object expectValue, Supplier<Object> actual) {
        waitUtil(expectValue, actual, 5000);
    }

    public static void waitUtil(Object expectValue, Supplier<Object> actual, long timeoutMillis) {
        waitUtil(expectValue, actual, timeoutMillis, null);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void waitUtil(Supplier<Boolean> actual, Executor executor) {
        waitUtil(Boolean.TRUE, (Supplier) actual, 5000, executor);
    }

    public static void waitUtil(Object expectValue, Supplier<Object> actual, Executor executor) {
        waitUtil(expectValue, actual, 5000, executor);
    }

    public static void waitUtil(Object expectValue, Supplier<Object> actual, long timeoutMillis, Executor executor) {
        long start = System.nanoTime();
        long deadline = start + timeoutMillis * 1000 * 1000;
        Object obj = getResultInExecutor(executor, actual);
        if (Objects.equals(expectValue, obj)) {
            return;
        }
        while (deadline - System.nanoTime() > 0) {
            obj = getResultInExecutor(executor, actual);
            if (Objects.equals(expectValue, obj)) {
                return;
            }
            try {
                //noinspection BusyWait
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        throw new AssertionFailedError("expect: " + expectValue + ", actual:" + obj + ", timeout="
                + timeoutMillis + "ms, cost=" + (System.nanoTime() - start) / 1000 / 1000 + "ms");
    }

    public static Object getResultInExecutor(Executor executor, Supplier<Object> actual) {
        if (executor == null) {
            return actual.get();
        }
        CompletableFuture<Object> f = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                f.complete(actual.get());
            } catch (Throwable e) {
                f.completeExceptionally(e);
            }
        });
        try {
            return f.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void updateTimestamp(Timestamp ts, long nanoTime, long wallClockMillis) {
        ts.nanoTime = nanoTime;
        ts.wallClockMillis = wallClockMillis;
    }

    public static void plus1Hour(Timestamp ts) {
        ts.nanoTime += TimeUnit.HOURS.toNanos(1);
        ts.wallClockMillis += TimeUnit.HOURS.toMillis(1);
    }

    public static void plus(Timestamp ts, int value, TimeUnit unit) {
        ts.nanoTime += unit.toNanos(value);
        ts.wallClockMillis += unit.toMillis(value);
    }

    public static RefBufferFactory heapPool() {
        ByteBufferPool p = new DefaultPoolFactory().createPool(new Timestamp(), false);
        return new RefBufferFactory(p, 0);
    }

    public static ByteBufferPool directPool() {
        return new DefaultPoolFactory().createPool(new Timestamp(), true);
    }

    public static String randomStr(int length) {
        Random r = new Random();
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (r.nextInt(26) + 'a');
        }
        return new String(bytes);
    }
}
