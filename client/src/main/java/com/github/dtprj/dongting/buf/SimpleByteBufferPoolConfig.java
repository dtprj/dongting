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
package com.github.dtprj.dongting.buf;

import com.github.dtprj.dongting.common.Timestamp;

/**
 * @author huangli
 */
public class SimpleByteBufferPoolConfig {
    private Timestamp ts;
    private boolean direct = false;
    private int threshold = SimpleByteBufferPool.DEFAULT_THRESHOLD;
    private boolean threadSafe = false;
    private int[] bufSizes = SimpleByteBufferPool.DEFAULT_BUF_SIZE;
    private int[] minCount = SimpleByteBufferPool.DEFAULT_MIN_COUNT;
    private int[] maxCount = SimpleByteBufferPool.DEFAULT_MAX_COUNT;
    private long timeoutMillis = 10 * 1000;
    private long shareSize = 0;

    public SimpleByteBufferPoolConfig() {
    }

    public SimpleByteBufferPoolConfig(Timestamp ts, boolean direct) {
        this.ts = ts;
        this.direct = direct;
    }

    public SimpleByteBufferPoolConfig(Timestamp ts, boolean direct, int threshold, boolean threadSafe) {
        this.ts = ts;
        this.direct = direct;
        this.threshold = threshold;
        this.threadSafe = threadSafe;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public boolean isDirect() {
        return direct;
    }

    public void setDirect(boolean direct) {
        this.direct = direct;
    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

    public boolean isThreadSafe() {
        return threadSafe;
    }

    public void setThreadSafe(boolean threadSafe) {
        this.threadSafe = threadSafe;
    }

    public int[] getBufSizes() {
        return bufSizes;
    }

    public void setBufSizes(int[] bufSizes) {
        this.bufSizes = bufSizes;
    }

    public int[] getMinCount() {
        return minCount;
    }

    public void setMinCount(int[] minCount) {
        this.minCount = minCount;
    }

    public int[] getMaxCount() {
        return maxCount;
    }

    public void setMaxCount(int[] maxCount) {
        this.maxCount = maxCount;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public long getShareSize() {
        return shareSize;
    }

    public void setShareSize(long shareSize) {
        this.shareSize = shareSize;
    }
}
