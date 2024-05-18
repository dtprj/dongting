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
    public static final int PERF_RPC_ACQUIRE = 1;
    public static final int PERF_RPC_WORKER_QUEUE = 2;
    public static final int PERF_RPC_CHANNEL_QUEUE = 3;

    protected final boolean useNanos;

    public PerfCallback(boolean useNanos) {
        this.useNanos = useNanos;
    }

    public long takeTime(int perfType, Timestamp ts) {
        if (!accept(perfType)) {
            return 0;
        }
        return takeTime(ts);
    }

    protected long takeTime(Timestamp ts) {
        if (useNanos) {
            if (ts == null) {
                return System.nanoTime();
            } else {
                ts.refresh(0);
                return ts.getNanoTime();
            }
        } else {
            if (ts == null) {
                return System.currentTimeMillis();
            } else {
                ts.refresh(1);
                return ts.getWallClockMillis();
            }
        }
    }

    public final void callDuration(int perfType, long start, Timestamp ts) {
        if (!accept(perfType)) {
            return;
        }
        long costTime = takeTime(ts) - start;
        duration(perfType, costTime);
    }

    protected abstract void duration(int perfType, long costTime);

    protected abstract boolean accept(int perfType);

}
