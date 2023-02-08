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
public class DtTime {
    private final long createTime;
    private final long deadline;

    public DtTime() {
        this.createTime = System.nanoTime();
        this.deadline = createTime;
    }

    public DtTime(long timeout, TimeUnit unit) {
        this.createTime = System.nanoTime();
        this.deadline = createTime + unit.toNanos(timeout);
    }

    public DtTime(Timestamp ts, long timeout, TimeUnit unit) {
        this.createTime = ts.getNanoTime();
        this.deadline = createTime + unit.toNanos(timeout);
    }

    public DtTime(long nanoTime, long timeout, TimeUnit unit) {
        this.createTime = nanoTime;
        this.deadline = createTime + unit.toNanos(timeout);
    }

    public long elapse(TimeUnit unit) {
        return unit.convert(System.nanoTime() - createTime, TimeUnit.NANOSECONDS);
    }

    public long rest(TimeUnit unit) {
        return unit.convert(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    public long rest(TimeUnit unit, Timestamp ts) {
        return unit.convert(deadline - ts.getNanoTime(), TimeUnit.NANOSECONDS);
    }

    public boolean isTimeout() {
        return deadline - System.nanoTime() <= 0;
    }

    public boolean isTimeout(Timestamp ts) {
        return deadline - ts.getNanoTime() <= 0;
    }

    public long getTimeout(TimeUnit unit) {
        return unit.convert(deadline - createTime, TimeUnit.NANOSECONDS);
    }
}
