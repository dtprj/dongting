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
    public final Timestamp ts;
    public final boolean direct;
    public final boolean threadSafe;
    public final int threshold;
    public final int[] bufSizes;
    public final int[] minCount;
    public final int[] maxCount;
    public final long timeoutMillis;
    public final long shareSize;

    public SimpleByteBufferPoolConfig(Timestamp ts, boolean direct) {
        this(ts, direct, SimpleByteBufferPool.DEFAULT_THRESHOLD, false);
    }

    public SimpleByteBufferPoolConfig(Timestamp ts, boolean direct, int threshold, boolean threadSafe) {
        this(ts, direct, threshold, threadSafe, SimpleByteBufferPool.DEFAULT_BUF_SIZE,
                SimpleByteBufferPool.DEFAULT_MIN_COUNT, SimpleByteBufferPool.DEFAULT_MAX_COUNT);
    }

    public SimpleByteBufferPoolConfig(Timestamp ts, boolean direct, int threshold, boolean threadSafe,
                                      int[] bufSizes, int[] minCount, int[] maxCount) {
        this(ts, direct, threshold, threadSafe, bufSizes, minCount, maxCount, 10 * 1000, 0);
    }

    public SimpleByteBufferPoolConfig(Timestamp ts, boolean direct, int threshold, boolean threadSafe, int[] bufSizes,
                                      int[] minCount, int[] maxCount, long timeoutMillis, long shareSize) {
        this.ts = ts;
        this.direct = direct;
        this.threshold = threshold;
        this.threadSafe = threadSafe;
        this.bufSizes = bufSizes;
        this.minCount = minCount;
        this.maxCount = maxCount;
        this.timeoutMillis = timeoutMillis;
        this.shareSize = shareSize;
    }

}
