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
public class Timestamp {
    private long nanoTime;
    private long wallClockMillis;

    public Timestamp() {
        nanoTime = System.nanoTime();
        wallClockMillis = System.currentTimeMillis();
    }

    public Timestamp(long nanoTime, long wallClockMillis) {
        this.nanoTime = nanoTime;
        this.wallClockMillis = wallClockMillis;
    }

    public long getNanoTime() {
        return nanoTime;
    }

    public long getWallClockMillis() {
        return wallClockMillis;
    }

    public void refresh(int millisDiff) {
        long t = System.currentTimeMillis();
        long old = this.wallClockMillis;
        if (t < old || t - old >= millisDiff) {
            this.wallClockMillis = t;
            this.nanoTime = System.nanoTime();
        }
    }
}
