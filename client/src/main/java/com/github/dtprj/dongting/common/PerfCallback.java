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

/**
 * @author huangli
 */
public abstract class PerfCallback {

    protected final boolean useNanos;

    public PerfCallback(boolean useNanos) {
        this.useNanos = useNanos;
    }

    public final boolean isUseNanos() {
        return useNanos;
    }

    public long takeTime(int perfType) {
        if (!accept(perfType)) {
            return 0;
        }
        return takeTime0();
    }

    public long takeTime(int perfType, Timestamp ts) {
        if (!accept(perfType)) {
            return 0;
        }
        return takeTime0(ts);
    }

    private long takeTime0() {
        if (useNanos) {
            return System.nanoTime();
        } else {
            return System.currentTimeMillis();
        }
    }

    private long takeTime0(Timestamp ts) {
        if (useNanos) {
            return ts.getNanoTime();
        } else {
            return ts.getWallClockMillis();
        }
    }

    public void callDuration(int perfType, long start, long value, Timestamp ts) {
        if (!accept(perfType)) {
            return;
        }
        long costTime = takeTime0(ts) - start;
        onDuration(perfType, costTime, value);
    }

    public void callDuration(int perfType, long start) {
        if (!accept(perfType)) {
            return;
        }
        long costTime = takeTime0() - start;
        onDuration(perfType, costTime, 0);
    }

    public void callDuration(int perfType, long start, long value) {
        if (!accept(perfType)) {
            return;
        }
        long costTime = takeTime0() - start;
        onDuration(perfType, costTime, value);
    }

    public void callCount(int perfType, long value) {
        if (!accept(perfType)) {
            return;
        }
        onCount(perfType, value);
    }

    public void callCount(int perfType) {
        if (!accept(perfType)) {
            return;
        }
        onCount(perfType, 1);
    }

    public abstract boolean accept(int perfType);

    public abstract void onDuration(int perfType, long costTime, long value);

    public abstract void onCount(int perfType, long value);

}
