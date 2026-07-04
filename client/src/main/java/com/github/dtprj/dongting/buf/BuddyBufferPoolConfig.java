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
public class BuddyBufferPoolConfig {

    public static final int DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024;
    public static final int DEFAULT_MIN_BLOCK_SIZE = 32 * 1024;

    public final boolean direct;
    public final int chunkSize;
    public final int minBlockSize;
    public final int minChunkCount;
    public final int maxChunkCount;
    public final long timeoutMillis;
    public final boolean threadSafe;

    public BuddyBufferPoolConfig(boolean direct, int chunkSize, int minBlockSize,
                                  int minChunkCount, int maxChunkCount, long timeoutMillis,
                                  boolean threadSafe) {
        this.direct = direct;
        if (chunkSize <= 0 || (chunkSize & (chunkSize - 1)) != 0) {
            throw new IllegalArgumentException("chunkSize must be a power of 2: " + chunkSize);
        }
        if (minBlockSize <= 0 || (minBlockSize & (minBlockSize - 1)) != 0) {
            throw new IllegalArgumentException("minBlockSize must be a power of 2: " + minBlockSize);
        }
        if (minBlockSize > chunkSize) {
            throw new IllegalArgumentException("minBlockSize > chunkSize");
        }
        if (minBlockSize < 16) {
            throw new IllegalArgumentException("minBlockSize too small: " + minBlockSize);
        }
        if (minChunkCount < 0) {
            throw new IllegalArgumentException("minChunkCount < 0");
        }
        if (maxChunkCount < 1 || maxChunkCount < minChunkCount) {
            throw new IllegalArgumentException("maxChunkCount invalid: " + maxChunkCount);
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis <= 0");
        }
        this.chunkSize = chunkSize;
        this.minBlockSize = minBlockSize;
        this.minChunkCount = minChunkCount;
        this.maxChunkCount = maxChunkCount;
        this.timeoutMillis = timeoutMillis;
        this.threadSafe = threadSafe;
    }
}
