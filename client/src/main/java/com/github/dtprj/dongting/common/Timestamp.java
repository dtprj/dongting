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

import com.github.dtprj.dongting.log.BugLog;

/**
 * @author huangli
 */
public class Timestamp {
    public long nanoTime;
    public long wallClockMillis;

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

    public boolean refresh(long millisDiff) {
        DtUtil.checkPositive(millisDiff, "millisDiff");
        long t = System.currentTimeMillis();
        long old = this.wallClockMillis;
        if (t - old >= millisDiff || t < old) {
            return update(t);
        } else {
            return false;
        }
    }

    private boolean update(long millis) {
        long newNano = System.nanoTime();
        if (newNano - this.nanoTime < 0) {
            // assert false, nanoTime() should not go back
            BugLog.getLog().error("nanoTime go back, old=" + this.nanoTime + ", new=" + newNano);
            return false;
        } else {
            this.wallClockMillis = millis;
            this.nanoTime = newNano;
            return true;
        }
    }

    public void refresh() {
        update(System.currentTimeMillis());
    }
}
