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
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Thread-safe large buffer pool using buddy allocation. Each {@link BuddyChunk} is a big
 * {@code ByteBuffer} carved into power-of-two blocks via {@code slice()}. Outstanding slices are
 * tracked in an {@link IdentityHashMap} so release can recover the chunk/offset without storing
 * metadata in the buffer data area.
 *
 * @author huangli
 */
public class BuddyBufferPool extends ByteBufferPool {
    static final VersionFactory VF = VersionFactory.getInstance();

    final int chunkSize;
    final int minBlockSize;
    private final int minChunkCount;
    private final int maxChunkCount;
    private final long timeoutNanos;

    private final List<BuddyChunk> chunks = new ArrayList<>();
    private final IdentityHashMap<ByteBuffer, BuddyChunk.BufInfo> bufMap = new IdentityHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    private final Consumer<RefBuffer> defaultReleasor = this::release;

    private long statBorrowCount;
    private long statBorrowHitCount;
    private long statReleaseCount;
    private long statReleaseHitCount;
    private long statNewChunkCount;
    private long statUnpooledCount;
    private long statChunkCleanCount;

    public BuddyBufferPool(BuddyBufferPoolConfig config) {
        super(config.direct, true, config.minBlockSize / 2, new Timestamp());
        this.chunkSize = config.chunkSize;
        this.minBlockSize = config.minBlockSize;
        this.minChunkCount = config.minChunkCount;
        this.maxChunkCount = config.maxChunkCount;
        this.timeoutNanos = config.timeoutMillis * 1000 * 1000;
        for (int i = 0; i < minChunkCount; i++) {
            chunks.add(newChunk());
        }
    }

    private BuddyChunk newChunk() {
        ByteBuffer root = allocate(chunkSize);
        return new BuddyChunk(root, chunkSize, minBlockSize);
    }

    @Override
    public RefBuffer borrow(boolean plain, int requestSize, int threshold) {
        return borrow0(plain, requestSize, threshold, defaultReleasor);
    }

    // the threshold parameter (caller's non-pooling hint) is ignored; buddy uses its own
    // this.threshold (= minBlockSize/2) to decide whether to pool.
    @Override
    RefBuffer borrow0(boolean plain, int requestSize, int threshold, Consumer<RefBuffer> releasor) {
        if (requestSize <= 0) {
            throw new IllegalArgumentException("requestSize must be positive: " + requestSize);
        }
        if (requestSize > chunkSize || requestSize <= this.threshold) {
            lock.lock();
            try {
                statBorrowCount++;
                statUnpooledCount++;
            } finally {
                lock.unlock();
            }
            return newUnpooledRefBuffer(plain, requestSize);
        }
        int targetSize = normalizeSize(requestSize);
        int targetLevel = Integer.numberOfTrailingZeros(targetSize / minBlockSize);

        ByteBuffer slice = null;
        boolean fallback = false;
        lock.lock();
        try {
            statBorrowCount++;
            BuddyChunk chunk = null;
            int offset = -1;
            for (BuddyChunk c : chunks) {
                offset = c.allocate(targetLevel);
                if (offset >= 0) {
                    chunk = c;
                    statBorrowHitCount++;
                    break;
                }
            }
            if (chunk == null && chunks.size() < maxChunkCount) {
                chunk = newChunk();
                chunks.add(chunk);
                statNewChunkCount++;
                offset = chunk.allocate(targetLevel);
            }
            if (chunk != null && offset >= 0) {
                slice = sliceBlock(chunk.rootBuffer, offset, targetSize);
                bufMap.put(slice, new BuddyChunk.BufInfo(chunk, offset));
            } else {
                statUnpooledCount++;
                fallback = true;
            }
        } finally {
            lock.unlock();
        }
        if (fallback) {
            return newUnpooledRefBuffer(plain, requestSize);
        }
        return new RefBuffer(plain, slice, releasor, false);
    }

    private int normalizeSize(int requestSize) {
        int s = minBlockSize;
        while (s < requestSize) {
            s <<= 1;
        }
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
        lock.lock();
        try {
            BuddyChunk.BufInfo info = bufMap.remove(buf);
            if (info == null) {
                releaseForeignBuffer(buf);
                return;
            }
            statReleaseCount++;
            statReleaseHitCount++;
            BuddyChunk chunk = info.chunk;
            chunk.free(info.offset, chunk.levelOfBlockSize(buf.capacity()));
            // ts.nanoTime is only refreshed in clean(); this value may lag real release time by up to
            // the scheduler interval, which is negligible vs timeoutNanos
            if (chunk.freeBytes == chunkSize && chunk.lastFullFreeNanos == 0) {
                chunk.lastFullFreeNanos = ts.nanoTime;
            }
        } finally {
            lock.unlock();
        }
    }

    // A buffer not found in bufMap. With releasor-based routing this is normally unreachable;
    // kept as a defensive fallback for direct/misuse calls. If capacity falls in buddy's range it
    // is treated as a double release of a buddy slice — fail loud. Otherwise release as chain tail.
    private void releaseForeignBuffer(ByteBuffer buf) {
        if (buf.capacity() >= minBlockSize) {
            throw new DtBugException("buffer does not belong to this pool or released twice, capacity="
                    + buf.capacity());
        }
        if (direct) {
            VF.releaseDirectBuffer(buf);
        }
    }

    @Override
    public void clean() {
        lock.lock();
        try {
            ts.refresh(1);
            long expireNanos = ts.nanoTime - timeoutNanos;
            Iterator<BuddyChunk> it = chunks.iterator();
            while (it.hasNext()) {
                if (chunks.size() <= minChunkCount) {
                    break;
                }
                BuddyChunk c = it.next();
                if (c.freeBytes == chunkSize && c.lastFullFreeNanos != 0
                        && c.lastFullFreeNanos <= expireNanos) {
                    it.remove();
                    statChunkCleanCount++;
                    if (direct) {
                        VF.releaseDirectBuffer(c.rootBuffer);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    Consumer<RefBuffer> getDefaultReleasor() {
        return defaultReleasor;
    }

    @Override
    public String formatStat() {
        lock.lock();
        try {
            DecimalFormat f = new DecimalFormat("#,###");
            return "chunks " + chunks.size() + "(min=" + minChunkCount + ",max=" + maxChunkCount + ")"
                    + ", chunkSize=" + chunkSize / 1024 + "KB"
                    + ", minBlock=" + minBlockSize / 1024 + "KB\n"
                    + "borrow " + f.format(statBorrowCount) + "(hit=" + f.format(statBorrowHitCount) + ")"
                    + ", release " + f.format(statReleaseCount) + "(hit=" + f.format(statReleaseHitCount) + ")\n"
                    + "newChunk " + f.format(statNewChunkCount)
                    + ", unpooled " + f.format(statUnpooledCount)
                    + ", chunkClean " + f.format(statChunkCleanCount);
        } finally {
            lock.unlock();
        }
    }
}
