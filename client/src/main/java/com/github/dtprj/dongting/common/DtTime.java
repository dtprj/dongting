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

import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class DtTime implements Comparable<DtTime> {
    public final long createTimeNanos;
    public final long deadlineNanos;

    public DtTime() {
        this.createTimeNanos = System.nanoTime();
        this.deadlineNanos = createTimeNanos;
    }

    public DtTime(long timeout, TimeUnit unit) {
        this.createTimeNanos = System.nanoTime();
        this.deadlineNanos = createTimeNanos + unit.toNanos(timeout);
    }

    public DtTime(Timestamp ts, long timeout, TimeUnit unit) {
        this.createTimeNanos = ts.nanoTime;
        this.deadlineNanos = createTimeNanos + unit.toNanos(timeout);
    }

    public DtTime(long nanoTime, long timeout, TimeUnit unit) {
        this.createTimeNanos = nanoTime;
        this.deadlineNanos = createTimeNanos + unit.toNanos(timeout);
    }

    @Override
    public int compareTo(DtTime o) {
        long x = this.deadlineNanos - o.deadlineNanos;
        return x < 0 ? -1 : (x == 0 ? 0 : 1);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DtTime)) {
            return false;
        }
        DtTime other = (DtTime) obj;
        return this.deadlineNanos == other.deadlineNanos && this.createTimeNanos == other.createTimeNanos;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(deadlineNanos) ^ Long.hashCode(createTimeNanos);
    }

    public long elapse(TimeUnit unit) {
        return unit.convert(System.nanoTime() - createTimeNanos, TimeUnit.NANOSECONDS);
    }

    public long rest(TimeUnit unit) {
        return unit.convert(deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    public long rest(TimeUnit unit, Timestamp ts) {
        return unit.convert(deadlineNanos - ts.nanoTime, TimeUnit.NANOSECONDS);
    }

    public boolean isTimeout() {
        return deadlineNanos - System.nanoTime() <= 0;
    }

    public boolean isTimeout(Timestamp ts) {
        return deadlineNanos - ts.nanoTime <= 0;
    }

    public long getTimeout(TimeUnit unit) {
        return unit.convert(deadlineNanos - createTimeNanos, TimeUnit.NANOSECONDS);
    }
}
