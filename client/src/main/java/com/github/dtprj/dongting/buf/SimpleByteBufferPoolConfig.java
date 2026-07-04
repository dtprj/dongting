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

/**
 * @author huangli
 */
public class SimpleByteBufferPoolConfig {
    public static final int DEFAULT_WEAK_REF_THRESHOLD = 4096;

    public final boolean direct;
    public final int threshold;
    public final int weakRefThreshold;
    public final int[] bufSizes;
    public final int[] minCount;
    public final int[] maxCount;
    public final long shareSize;

    public SimpleByteBufferPoolConfig(boolean direct, int threshold,
                                      int[] bufSizes, int[] minCount, int[] maxCount) {
        this(direct, threshold, DEFAULT_WEAK_REF_THRESHOLD, bufSizes, minCount, maxCount, 0);
    }

    public SimpleByteBufferPoolConfig(boolean direct, int threshold, int[] bufSizes,
                                      int[] minCount, int[] maxCount, long shareSize) {
        this(direct, threshold, DEFAULT_WEAK_REF_THRESHOLD, bufSizes, minCount, maxCount, shareSize);
    }

    public SimpleByteBufferPoolConfig(boolean direct, int threshold, int weakRefThreshold,
                                      int[] bufSizes, int[] minCount, int[] maxCount, long shareSize) {
        this.direct = direct;
        this.threshold = threshold;
        this.weakRefThreshold = weakRefThreshold;
        this.bufSizes = bufSizes;
        this.minCount = minCount;
        this.maxCount = maxCount;
        this.shareSize = shareSize;
    }

}
