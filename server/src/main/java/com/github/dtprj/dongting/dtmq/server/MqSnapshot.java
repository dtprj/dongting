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
package com.github.dtprj.dongting.dtmq.server;

import com.github.dtprj.dongting.buf.Buffers;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;

import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * @author huangli
 */
class MqSnapshot extends Snapshot {

    static final int ITEM_BYTES = 16;
    static final int PAGE_BYTES = 64 * 1024;

    private final FiberGroup fiberGroup;
    private final LinkedList<RefBuffer> pages = new LinkedList<>();
    private RefBuffer writePage;

    MqSnapshot(SnapshotInfo snapshotInfo, LongObjMap<QueueIdxInfo> queues, FiberGroup fiberGroup) {
        super(snapshotInfo);
        this.fiberGroup = fiberGroup;
        Buffers buffers = fiberGroup.dispatcher.thread.buffers;
        try {
            queues.forEach((queueId, q) -> {
                if (writePage == null) {
                    writePage = buffers.borrowLocal(PAGE_BYTES);
                }
                ByteBuffer buf = writePage.getBuffer();
                buf.putLong(queueId);
                buf.putLong(q.nextSeq);
                if (!buf.hasRemaining()) {
                    buf.flip();
                    pages.add(writePage);
                    writePage = null;
                }
            });
            if (writePage != null) {
                writePage.getBuffer().flip();
                pages.add(writePage);
                writePage = null;
            }
        } catch (Throwable t) {
            if (writePage != null) {
                writePage.release();
            }
            close();
            throw t;
        }
    }

    static void parseChunk(ByteBuffer data, MqIdxManager manager) {
        if (data.remaining() % ITEM_BYTES != 0) {
            throw new RaftException("bad mq snapshot data len: " + data.remaining());
        }
        while (data.hasRemaining()) {
            manager.register(data.getLong(), data.getLong());
        }
    }

    @Override
    public FiberFuture<Integer> readNext(ByteBuffer buffer) {
        int bytes = 0;
        RefBuffer p;
        while (buffer.remaining() >= ITEM_BYTES && (p = pages.peek()) != null) {
            ByteBuffer src = p.getBuffer();
            while (buffer.remaining() >= ITEM_BYTES && src.hasRemaining()) {
                buffer.putLong(src.getLong());
                buffer.putLong(src.getLong());
                bytes += ITEM_BYTES;
            }
            if (!src.hasRemaining()) {
                pages.poll();
                p.release();
            }
        }
        return FiberFuture.completedFuture(fiberGroup, bytes);
    }

    @Override
    protected void doClose() {
        RefBuffer p;
        while ((p = pages.poll()) != null) {
            p.release();
        }
    }
}
