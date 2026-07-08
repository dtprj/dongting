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

import com.github.dtprj.dongting.common.DtBugException;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.common.VersionFactory;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 *
 * @author huangli
 */
public class BuddyBufferPool extends ByteBufferPool {

    // refresh periodically; package-private so tests can advance the clock
    final Timestamp ts;

    final int chunkSize;
    final int minBlockSize;
    private final int minChunkCount;
    private final int maxChunkCount;
    private final long timeoutNanos;
    final boolean threadSafe;
    private final GlobalIdleChunkList globalIdleChunkList;

    private final ArrayList<BuddyChunk> chunks = new ArrayList<>();
    private final IdentityHashMap<ByteBuffer, BuddyChunk.BufInfo> bufMap = new IdentityHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    // last chunk that satisfied an allocate; tried first on the next borrow so the common case
    // (same chunk still has room) stays O(1) instead of scanning the whole chunk list.
    private BuddyChunk hintChunk;

    private final Consumer<RefBuffer> defaultReleasor = this::release;

    private long statBorrowCount;
    private long statBorrowHitCount;
    private long statReleaseCount;
    private long statReleaseHitCount;
    private long statNewChunkCount;
    private long statUnpooledCount;
    private long statChunkCleanCount;
    private long statChunkListHitCount;

    private boolean destroyed;

    public BuddyBufferPool(BuddyBufferPoolConfig config, GlobalIdleChunkList globalIdleChunkList) {
        super(config.direct, config.minBlockSize / 2);
        Objects.requireNonNull(globalIdleChunkList);
        this.ts = config.ts;
        this.chunkSize = config.chunkSize;
        this.minBlockSize = config.minBlockSize;
        this.minChunkCount = config.minChunkCount;
        this.maxChunkCount = config.maxChunkCount;
        this.timeoutNanos = config.timeoutMillis * 1000 * 1000;
        this.threadSafe = config.threadSafe;
        this.globalIdleChunkList = globalIdleChunkList;
    }

    private void lock() {
        if (threadSafe) {
            lock.lock();
        }
    }

    private void unlock() {
        if (threadSafe) {
            lock.unlock();
        }
    }

    @Override
    public RefBuffer borrow(boolean plain, int requestSize, int threshold) {
        return borrow(plain, requestSize, threshold, defaultReleasor);
    }

    @Override
    RefBuffer borrow(boolean plain, int requestSize, int threshold, Consumer<RefBuffer> releasor) {
        if (requestSize <= 0) {
            throw new IllegalArgumentException("requestSize must be positive: " + requestSize);
        }
        if (requestSize > chunkSize || requestSize <= this.threshold) {
            lock();
            try {
                statBorrowCount++;
                statUnpooledCount++;
            } finally {
                unlock();
            }
            return newUnpooledRefBuffer(plain, requestSize);
        }
        int targetSize = normalizeSize(requestSize);
        int targetLevel = Integer.numberOfTrailingZeros(targetSize / minBlockSize);

        ByteBuffer slice;
        lock();
        try {
            slice = doBorrow(targetLevel, targetSize);
        } finally {
            unlock();
        }
        if (slice == null) {
            return newUnpooledRefBuffer(plain, requestSize);
        }
        return new RefBuffer(plain, slice, releasor, false);
    }

    private ByteBuffer doBorrow(int targetLevel, int targetSize) {
        statBorrowCount++;
        BuddyChunk chunk = null;
        int offset = -1;
        // fast path: the chunk that served the previous borrow usually still has room
        if (hintChunk != null && hintChunk.maxAvailableLevel >= targetLevel) {
            offset = hintChunk.allocate(targetLevel);
            if (offset >= 0) {
                chunk = hintChunk;
                statBorrowHitCount++;
            }
        }
        if (chunk == null) {
            for (int size = chunks.size(), i = 0; i < size; i++) {
                BuddyChunk c = chunks.get(i);
                if (c.maxAvailableLevel < targetLevel) {
                    continue;
                }
                offset = c.allocate(targetLevel);
                if (offset >= 0) {
                    chunk = c;
                    hintChunk = chunk;
                    statBorrowHitCount++;
                    break;
                }
            }
        }
        if (chunk == null) {
            BuddyChunk cached = globalIdleChunkList.borrowIdleChunk();
            if (cached != null) {
                statChunkListHitCount++;
                chunks.add(cached);
                if (chunks.size() <= maxChunkCount) {
                    // If the value is less than or equal to maxChunkCount, no budget will be occupied.
                    // However, the chunk has already had its budget allocated in globalIdleChunkList,
                    // so the corresponding budget needs to be returned.
                    globalIdleChunkList.release(chunkSize);
                }
                chunk = cached;
                offset = chunk.allocate(targetLevel); // new chunk allocate should success
                hintChunk = chunk;
            }
        }
        if (chunk == null) {
            if (chunks.size() < maxChunkCount || globalIdleChunkList.borrow(chunkSize)) {
                try {
                    chunk = new BuddyChunk(direct, chunkSize, minBlockSize);
                } catch (OutOfMemoryError e) {
                    if (chunks.size() >= maxChunkCount) {
                        globalIdleChunkList.release(chunkSize);
                    }
                    statUnpooledCount++;
                    return null;
                }
                chunks.add(chunk);
                statNewChunkCount++;
                offset = chunk.allocate(targetLevel); // new chunk allocate should success
                hintChunk = chunk;
            }
        }
        if (chunk != null && offset >= 0) {
            ByteBuffer slice = sliceBlock(chunk.rootBuffer, offset, targetSize);
            bufMap.put(slice, new BuddyChunk.BufInfo(chunk, offset));
            return slice;
        } else {
            statUnpooledCount++;
            return null;
        }
    }

    private int normalizeSize(int requestSize) {
        int s = Integer.highestOneBit(requestSize - 1) << 1;
        if (s == 0) s = minBlockSize;
        return s;
    }

    private ByteBuffer sliceBlock(ByteBuffer root, int offset, int size) {
        int oldLimit = root.limit();
        int oldPos = root.position();
        root.limit(offset + size);
        root.position(offset);
        ByteBuffer sl = root.slice();
        root.limit(oldLimit);
        root.position(oldPos);
        return sl;
    }

    @Override
    void releaseBuffer(ByteBuffer buf) {
        lock();
        try {
            BuddyChunk.BufInfo info = bufMap.remove(buf);
            if (info == null) {
                throw new DtBugException("buffer does not belong to this pool or released twice");
            }
            statReleaseCount++;
            statReleaseHitCount++;
            BuddyChunk chunk = info.chunk;
            chunk.free(info.offset, chunk.levelOfBlockSize(buf.capacity()));
            if (chunk.freeBytes == chunkSize) {
                if (destroyed) {
                    shrink();
                } else {
                    chunk.lastFullFreeNanos = ts.nanoTime;
                }
            }
        } finally {
            unlock();
        }
    }

    @Override
    public void shrink() {
        lock();
        try {
            long nanoTime = ts.nanoTime;
            Iterator<BuddyChunk> it = chunks.iterator();
            while (it.hasNext()) {
                if (!destroyed && chunks.size() <= minChunkCount) {
                    break;
                }
                BuddyChunk c = it.next();
                if (c.freeBytes == chunkSize && (destroyed || (nanoTime - c.lastFullFreeNanos > timeoutNanos))) {
                    it.remove();
                    if (hintChunk == c) {
                        hintChunk = null;
                    }
                    statChunkCleanCount++;
                    if (chunks.size() >= maxChunkCount || globalIdleChunkList.borrow(chunkSize)) {
                        c.lastFullFreeNanos = nanoTime;
                        globalIdleChunkList.returnIdleChunk(c);
                    } else if (direct) {
                        // drop
                        VersionFactory.getInstance().releaseDirectBuffer(c.rootBuffer);
                    }
                }
            }
        } finally {
            unlock();
        }
    }

    public void destroy() {
        lock();
        try {
            destroyed = true;
            shrink();
        } finally {
            unlock();
        }
    }

    @Override
    public String formatStat() {
        lock();
        try {
            DecimalFormat f = new DecimalFormat("#,###");
            return "chunks " + chunks.size() + "(min=" + minChunkCount + ",max=" + maxChunkCount + ")"
                    + ", chunkSize=" + chunkSize / 1024 + "KB"
                    + ", minBlock=" + minBlockSize / 1024 + "KB\n"
                    + "borrow " + f.format(statBorrowCount) + "(hit=" + f.format(statBorrowHitCount) + ")"
                    + ", release " + f.format(statReleaseCount) + "(hit=" + f.format(statReleaseHitCount) + ")\n"
                    + "newChunk " + f.format(statNewChunkCount)
                    + ", chunkListHit " + f.format(statChunkListHitCount)
                    + ", unpooled " + f.format(statUnpooledCount)
                    + ", chunkClean " + f.format(statChunkCleanCount);
        } finally {
            unlock();
        }
    }
}
