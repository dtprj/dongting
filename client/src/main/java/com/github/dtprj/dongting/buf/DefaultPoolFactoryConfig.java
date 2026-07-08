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
public class DefaultPoolFactoryConfig {

    /** Requests not larger than this are allocated directly instead of being pooled. */
    public int threshold = 64;

    /** Size buckets for the small buffer pool, must be sorted ascending. */
    public int[] smallSize = new int[]{128, 192, 256, 384, 512, 768, 1024, 1536,
            2 * 1024, 3 * 1024, 4 * 1024, 6 * 1024, 8 * 1024, 12 * 1024};

    public long smallPoolSlotMinSize = 768 * 1024; // 14 slots total 10752KB
    public long smallPoolSlotMaxSize = smallPoolSlotMinSize * 2;
    public long smallPoolShareSize = smallPoolSlotMinSize * smallSize.length;

    public int globalMinChunkCount = 4;
    public int globalMaxChunkCount = 8;
    public long largeShare = 400 * 1024 * 1024;

    public long largePoolGlobalTimeoutMillis = 60_000;
    public long largePoolTimeoutMillis = 2000;

    public int chunkSize = 4 * 1024 * 1024;
    public int minBlockSize = 16 * 1024;
}
