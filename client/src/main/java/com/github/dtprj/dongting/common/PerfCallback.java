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

import com.github.dtprj.dongting.net.PerfConsts;

/**
 * @author huangli
 */
public abstract class PerfCallback implements PerfConsts {

    protected final boolean useNanos;

    public PerfCallback(boolean useNanos) {
        this.useNanos = useNanos;
    }

    public final void refresh(Timestamp ts) {
        if (useNanos) {
            ts.refresh();
        } else {
            ts.refresh(1);
        }
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

    public void fireTime(int perfType, long startTime, int count, long sum, Timestamp ts) {
        if (startTime == 0 || !accept(perfType)) {
            return;
        }
        long costTime = takeTime0(ts) - startTime;
        onEvent(perfType, costTime, count, sum);
    }

    public void fireTime(int perfType, long startTime) {
        if (startTime == 0 || !accept(perfType)) {
            return;
        }
        long costTime = takeTime0() - startTime;
        onEvent(perfType, costTime, 1, 0);
    }

    public void fireTime(int perfType, long startTime, int count, long sum) {
        if (startTime == 0 || !accept(perfType)) {
            return;
        }
        long costTime = takeTime0() - startTime;
        onEvent(perfType, costTime, count, sum);
    }

    public void fire(int perfType, int count, long sum) {
        if (!accept(perfType)) {
            return;
        }
        onEvent(perfType, 0, count, sum);
    }

    public void fire(int perfType) {
        if (!accept(perfType)) {
            return;
        }
        onEvent(perfType, 0, 1, 0);
    }

    public abstract boolean accept(int perfType);

    public abstract void onEvent(int perfType, long costTime, int count, long sum);

}
