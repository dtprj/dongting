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

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Buddy allocator over a single large {@link ByteBuffer} chunk. Level 0 is {@code minBlockSize};
 * each level doubles the size; {@code maxLevel} is the whole chunk. Free blocks at each level are
 * tracked by block index in a {@link BitSet} (bit i = block i free), so allocate/free do only
 * bit operations with no boxing.
 *
 * @author huangli
 */
class BuddyChunk {
    final ByteBuffer rootBuffer;
    final int chunkSize;
    final int minBlockSize;
    final int maxLevel;
    final BitSet[] freeLists;
    long freeBytes;
    long lastFullFreeNanos;

    BuddyChunk(ByteBuffer rootBuffer, int chunkSize, int minBlockSize) {
        this.rootBuffer = rootBuffer;
        this.chunkSize = chunkSize;
        this.minBlockSize = minBlockSize;
        this.maxLevel = Integer.numberOfTrailingZeros(chunkSize / minBlockSize);
        this.freeLists = new BitSet[maxLevel + 1];
        for (int i = 0; i <= maxLevel; i++) {
            freeLists[i] = new BitSet();
        }
        freeLists[maxLevel].set(0);
        this.freeBytes = chunkSize;
    }

    int blockSize(int level) {
        return minBlockSize << level;
    }

    int levelOfBlockSize(int blockSize) {
        return Integer.numberOfTrailingZeros(blockSize / minBlockSize);
    }

    /**
     * @return start offset of an allocated block, or -1 if no block can satisfy the request.
     */
    int allocate(int targetLevel) {
        BitSet[] freeLists = this.freeLists;
        for (int lv = targetLevel; lv <= maxLevel; lv++) {
            BitSet fl = freeLists[lv];
            int blockIdx = fl.nextSetBit(0);
            if (blockIdx >= 0) {
                fl.clear(blockIdx);
                // split down: keep left half (2*idx) for the request, free right buddy (2*idx+1)
                while (lv > targetLevel) {
                    lv--;
                    freeLists[lv].set(blockIdx * 2 + 1);
                    blockIdx = blockIdx * 2;
                }
                freeBytes -= blockSize(targetLevel);
                return blockIdx * blockSize(targetLevel);
            }
        }
        return -1;
    }

    /**
     * Free a block at {@code offset} whose size is {@code blockSize(targetLevel)}, coalescing with
     * its buddy recursively. {@code freeBytes} is incremented by the released block size only;
     * coalesced buddies were already free and remain free.
     */
    void free(int offset, int targetLevel) {
        int lv = targetLevel;
        int blockIdx = offset / blockSize(lv);
        BitSet[] freeLists = this.freeLists;
        while (lv < maxLevel) {
            int buddy = blockIdx ^ 1;
            if (freeLists[lv].get(buddy)) {
                freeLists[lv].clear(buddy);
                blockIdx >>>= 1;
                lv++;
            } else {
                break;
            }
        }
        freeLists[lv].set(blockIdx);
        freeBytes += blockSize(targetLevel);
    }

    /**
     * Metadata for an outstanding slice of this chunk, stored in the pool's identity map so release
     * can recover the owning chunk and offset.
     */
    static class BufInfo {
        final BuddyChunk chunk;
        final int offset;

        BufInfo(BuddyChunk chunk, int offset) {
            this.chunk = chunk;
            this.offset = offset;
        }
    }
}
