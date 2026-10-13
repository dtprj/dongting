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

import com.github.dtprj.dongting.log.BugLog;

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
public class BuddyChunk {
    private static final int MAGIC = 0xD4195A29;

    final ByteBuffer rootBuffer;
    final int chunkSize;
    final int minBlockSize;
    final int maxLevel;
    final BitSet[] freeLists;
    long freeBytes;
    long lastFullFreeNanos;
    int maxAvailableLevel;
    boolean corrupted;

    BuddyChunk(boolean direct, int chunkSize, int minBlockSize) {
        if (chunkSize <= 0 || (chunkSize & (chunkSize - 1)) != 0 || chunkSize < minBlockSize) {
            throw new IllegalArgumentException(
                    "chunkSize must be a power of 2 and not less than minBlockSize: " + chunkSize);
        }
        if (minBlockSize < 16 || (minBlockSize & (minBlockSize - 1)) != 0) {
            throw new IllegalArgumentException("minBlockSize must be a power of 2 and >= 16: " + minBlockSize);
        }
        this.rootBuffer = direct ? ByteBuffer.allocateDirect(chunkSize) : ByteBuffer.allocate(chunkSize);
        this.chunkSize = chunkSize;
        this.minBlockSize = minBlockSize;
        this.maxLevel = Integer.numberOfTrailingZeros(chunkSize / minBlockSize);
        this.freeLists = new BitSet[maxLevel + 1];
        for (int i = 0; i <= maxLevel; i++) {
            freeLists[i] = new BitSet();
        }
        freeLists[maxLevel].set(0);
        rootBuffer.putInt(0, MAGIC);
        this.freeBytes = chunkSize;
        this.maxAvailableLevel = maxLevel;
    }

    private int blockSize(int level) {
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
                int foundLevel = lv;
                int bs = blockSize(lv);
                int off = blockIdx * bs;
                int v = rootBuffer.getInt(off);
                if (v != MAGIC) {
                    corrupted = true;
                    BugLog.logAndThrow("A bug may exist where the buffer is written to after release."
                            + " chunk=0x" + Integer.toHexString(System.identityHashCode(rootBuffer))
                            + ", offset=" + off + ", blockSize=" + bs
                            + ", expectedMagic=0x" + Integer.toHexString(MAGIC)
                            + ", actual=0x" + Integer.toHexString(v));
                }
                fl.clear(blockIdx);
                // split down: keep left half (2*idx) for the request, free right buddy (2*idx+1)
                while (lv > targetLevel) {
                    lv--;
                    int rightIdx = (blockIdx << 1) + 1;
                    freeLists[lv].set(rightIdx);
                    rootBuffer.putInt(rightIdx * blockSize(lv), MAGIC);
                    blockIdx = blockIdx << 1;
                }
                if (foundLevel == maxAvailableLevel && freeLists[foundLevel].isEmpty()) {
                    if (foundLevel > targetLevel) {
                        maxAvailableLevel--;
                    } else {
                        while (maxAvailableLevel >= 0 && freeLists[maxAvailableLevel].isEmpty()) {
                            maxAvailableLevel--;
                        }
                    }
                }
                freeBytes -= blockSize(targetLevel);
                rootBuffer.putInt(off, 0);
                return off;
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
        rootBuffer.putInt(offset, MAGIC);
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
        if (lv > maxAvailableLevel) {
            maxAvailableLevel = lv;
        }
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
