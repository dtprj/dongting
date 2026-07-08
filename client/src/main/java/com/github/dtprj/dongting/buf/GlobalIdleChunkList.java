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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.VersionFactory;

import java.util.ArrayList;
import java.util.LinkedList;

/**
 * @author huangli
 */
public class GlobalIdleChunkList extends ShareBudget implements Runnable {

    private final int targetSize;
    private final int chunkSize;
    private final int minBlockSize;
    private final boolean direct;

    private final LinkedList<BuddyChunk> idleChunks = new LinkedList<>();
    // weak-ref overflow cache for heap chunks; null for direct buffers (iceberg objects)
    private final WeakRefCache<BuddyChunk> weakRefCache;
    private final long timeoutNanos;

    GlobalIdleChunkList(long total, boolean direct, int chunkSize, int minBlockSize, long timeoutMillis,
                        int targetSize) {
        super(total, true);
        this.targetSize = targetSize;
        this.chunkSize = chunkSize;
        this.minBlockSize = minBlockSize;
        this.direct = direct;
        this.timeoutNanos = timeoutMillis * 1_000_000;
        this.weakRefCache = direct ? null : new WeakRefCache<>(16);
    }


    @Override
    public void run() {
        long now = System.nanoTime();
        int needAllocate = 0;
        LinkedList<BuddyChunk> destroyList = null;
        synchronized (this) {
            while (idleChunks.size() > targetSize) {
                BuddyChunk chunk = idleChunks.peekFirst();
                if (now - chunk.lastFullFreeNanos > timeoutNanos) {
                    idleChunks.pollFirst();
                    release0(chunkSize);
                    if (weakRefCache != null) {
                        // park in weak-ref cache so the chunk can be reclaimed if still referenced
                        weakRefCache.moveIdleElementsToCache(chunk);
                    } else {
                        if (destroyList == null) {
                            destroyList = new LinkedList<>();
                        }
                        destroyList.add(chunk);
                    }
                } else {
                    break;
                }
            }
            // reclaim cached chunks first so we avoid fresh allocation when possible
            if (weakRefCache != null) {
                while (idleChunks.size() < targetSize) {
                    if (!borrow0(chunkSize)) {
                        break;
                    }
                    BuddyChunk cached = weakRefCache.borrow();
                    if (cached == null) {
                        release0(chunkSize);
                        break;
                    }
                    cached.lastFullFreeNanos = now;
                    idleChunks.addLast(cached);
                }
                weakRefCache.cleanHeadAndTail();
            }
            for (int i = 0; i < targetSize - idleChunks.size(); i++) {
                if (borrow0(chunkSize)) {
                    needAllocate++;
                }
            }
        }

        if (destroyList != null) {
            for (BuddyChunk chunk : destroyList) {
                destroy(chunk);
            }
        }

        if (needAllocate > 0) {
            // allocate outside synchronized block
            ArrayList<BuddyChunk> chunks = new ArrayList<>(needAllocate);
            boolean oom = false;
            for (int i = 0; i < needAllocate; i++) {
                if (oom) {
                    release(chunkSize);
                } else {
                    BuddyChunk chunk;
                    try {
                        chunk = new BuddyChunk(direct, chunkSize, minBlockSize);
                        chunk.lastFullFreeNanos = now;
                        chunks.add(chunk);
                    } catch (OutOfMemoryError e) {
                        oom = true;
                        release(chunkSize);
                    }
                }
            }

            synchronized (this) {
                idleChunks.addAll(chunks);
            }
        }
    }

    public BuddyChunk borrowIdleChunk() {
        BuddyChunk chunk;
        boolean schedule;
        synchronized (this) {
            if (!idleChunks.isEmpty()) {
                chunk = idleChunks.pollLast();
            } else if (weakRefCache != null && borrow0(chunkSize)) {
                chunk = weakRefCache.borrow();
                if (chunk == null) {
                    release0(chunkSize);
                }
            } else {
                chunk = null;
            }
            schedule = idleChunks.isEmpty();
        }
        if (schedule) {
            DtUtil.LOW_PRIORITY_SCHEDULER.execute(this);
        }
        return chunk;
    }

    public void returnIdleChunk(BuddyChunk chunk) {
        boolean schedule;
        synchronized (this) {
            idleChunks.addLast(chunk);
            schedule = idleChunks.size() > targetSize;
        }
        if (schedule) {
            DtUtil.LOW_PRIORITY_SCHEDULER.execute(this);
        }
    }

    public void clear() {
        synchronized (this) {
            while (!idleChunks.isEmpty()) {
                BuddyChunk chunk = idleChunks.pollFirst();
                destroy(chunk);
            }
        }
    }

    private void destroy(BuddyChunk chunk) {
        if (direct) {
            VersionFactory.getInstance().releaseDirectBuffer(chunk.rootBuffer);
        }
    }

}
