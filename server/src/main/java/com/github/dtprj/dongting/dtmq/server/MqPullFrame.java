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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.net.DtChannel;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftTimeoutException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.raft.store.RaftLog;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

/** Returns a pre-encoded response; the caller sends it or calls clean(). */
public final class MqPullFrame extends FiberFrame<MqPullResp> {
    /** Truncated queues return only a retry cursor. */
    public static final class QueueResult {
        public final long queueId;
        final long requestedSeq;
        public long fromSeq;
        public long nextSeq;
        public int count;
        public boolean truncated;
        long resetPos;
        boolean needsIo;
        int memoryStart;
        int memoryCount;
        int ioStart;

        private QueueResult(long queueId, long requestedSeq) {
            this.queueId = queueId;
            this.requestedSeq = requestedSeq;
            reset();
        }

        private void reset() {
            fromSeq = requestedSeq;
            nextSeq = requestedSeq;
            count = 0;
            truncated = false;
            resetPos = Long.MAX_VALUE;
            needsIo = false;
            memoryStart = 0;
            memoryCount = 0;
            ioStart = 0;
        }
    }

    private final DtMQ mq;
    final RaftGroupConfigEx groupConfig;
    final RaftStatusImpl raftStatus;
    final RaftLog raftLog;
    final long[] queueIds;
    final long[] fromSeqs;
    // created lazily, one entry per scanned queue; null entry means idle and encodes from fromSeqs
    QueueResult[] results;
    final int limit;
    final int bytesLimit;
    private final long waitNanos;
    private final DtTime deadline;
    private FiberCondition changed;
    MqIdxManager manager;
    DtChannel channel;
    private boolean cancelled;
    private boolean waiting;
    private long waitEnd;
    private long changeVersion;
    private int queueIndex;
    private long[] positions;
    private int[] lengths;
    private RefBuffer[] cached;
    int count;
    private int bytes;
    private int cachedZeroCopyBytes;
    private int bodySize;
    boolean retry;
    private RefBuffer buffer;
    private List<ByteBuffer> segments;

    MqPullFrame(DtMQ mq, RaftGroupConfigEx groupConfig, RaftLog raftLog,
                long[] queueIds, long[] fromSeqs, int limit, int bytesLimit, long waitMillis, DtTime deadline) {
        if (queueIds.length == 0 || queueIds.length != fromSeqs.length) {
            throw new IllegalArgumentException("queueIds and fromSeqs must have the same nonzero length");
        }
        DtUtil.checkPositive(limit, "limit");
        DtUtil.checkPositive(bytesLimit, "bytesLimit");
        if (waitMillis < 0 || waitMillis > Long.MAX_VALUE / 1_000_000) {
            throw new IllegalArgumentException("invalid waitMillis: " + waitMillis);
        }
        this.mq = mq;
        this.groupConfig = groupConfig;
        this.raftStatus = (RaftStatusImpl) groupConfig.raftStatus;
        this.raftLog = raftLog;
        this.limit = limit;
        this.bytesLimit = bytesLimit;
        this.waitNanos = TimeUnit.MILLISECONDS.toNanos(waitMillis);
        this.deadline = Objects.requireNonNull(deadline);
        this.queueIds = queueIds;
        this.fromSeqs = fromSeqs;
        int len = queueIds.length;
        // zero-allocation pairwise check for small requests; a single sort copy for large ones
        if (len <= 16) {
            for (int i = 1; i < len; i++) {
                for (int j = 0; j < i; j++) {
                    if (queueIds[j] == queueIds[i]) {
                        throw new IllegalArgumentException("duplicate queueId: " + queueIds[i]);
                    }
                }
            }
        } else {
            long[] ids = queueIds.clone();
            Arrays.sort(ids);
            for (int i = 1; i < len; i++) {
                if (ids[i] == ids[i - 1]) {
                    throw new IllegalArgumentException("duplicate queueId: " + ids[i]);
                }
            }
        }
        for (int i = 0; i < len; i++) {
            if (fromSeqs[i] < 0) {
                throw new IllegalArgumentException("negative fromSeq: " + fromSeqs[i]);
            }
        }
    }

    /** Raft thread only; does not interrupt in-flight I/O or release its buffers prematurely. */
    public void cancel() {
        cancelled = true;
        if (changed != null) {
            changed.signalAll();
        }
    }

    void signal() {
        if (changed != null) {
            changed.signalAll();
        }
    }

    @Override
    public FrameCallResult execute(Void input) {
        if (getFiber().isDaemon()) {
            throw new RaftException("mq pull should not run in daemon fiber");
        }
        manager = mq.manager;
        mq.activePulls.add(this);
        return scan(null);
    }

    void checkCancelled() {
        if (cancelled || isGroupShouldStopPlain() || raftStatus.installSnapshot
                || manager == null || manager.markClose || manager != mq.manager
                || (channel != null && !channel.getChannel().isOpen())) {
            throw new CancellationException("mq pull cancelled");
        }
        if (deadline.isTimeout()) {
            throw new RaftTimeoutException("mq pull timeout");
        }
    }

    private FrameCallResult scan(Void unused) {
        releaseBuffers();
        checkCancelled();
        if (waitEnd == 0) {
            // fresh anchor; a stale one would shorten the wait
            groupConfig.ts.refresh(1);
            waitEnd = groupConfig.ts.nanoTime + waitNanos;
        }
        changeVersion = mq.changeVersion;
        queueIndex = 0;
        count = 0;
        bytes = 0;
        cachedZeroCopyBytes = 0;
        retry = false;
        if (results != null) {
            for (QueueResult r : results) {
                if (r != null) {
                    r.reset();
                }
            }
        }
        for (int i = 0; i < queueIds.length && hasCapacity(LogHeader.ITEM_HEADER_SIZE); i++) {
            MqIdxReader.scanMemory(this, manager.queues.get(queueIds[i]), i);
            QueueResult r = results == null ? null : results[i];
            if (r != null) {
                r.memoryCount = r.count;
            }
        }
        return readQueue(null);
    }

    private FrameCallResult readQueue(Void unused) {
        checkCancelled();
        if (retry) {
            return Fiber.resume(null, this::scan);
        }
        while (results != null && queueIndex < results.length && hasCapacity(LogHeader.ITEM_HEADER_SIZE)) {
            QueueResult r = results[queueIndex++];
            if (r != null && r.needsIo) {
                r.ioStart = count;
                return Fiber.call(new MqIdxReader(this, manager.queues.get(r.queueId), r), this::readQueue);
            }
        }
        if (!resetPositionsValid()) {
            return Fiber.resume(null, this::scan);
        }
        if (count > 0) {
            return prepareResponse();
        }
        if (results != null) {
            for (QueueResult r : results) {
                if (r != null && r.truncated) {
                    return prepareResponse();
                }
            }
        }
        long rest = waitEnd - groupConfig.ts.nanoTime;
        if (waitNanos == 0 || rest <= 0) {
            return prepareResponse();
        }
        if (mq.changeVersion != changeVersion) {
            return Fiber.resume(null, this::scan);
        }
        watch();
        rest = Math.min(rest, deadline.rest(TimeUnit.NANOSECONDS));
        if (rest <= 0) {
            throw new RaftTimeoutException("mq pull timeout");
        }
        return changed.await(rest, TimeUnit.NANOSECONDS, groupConfig.fiberGroup.shouldStopCondition, this::scan);
    }

    QueueResult result(int index) {
        if (results == null) {
            results = new QueueResult[queueIds.length];
        }
        QueueResult r = results[index];
        if (r == null) {
            r = results[index] = new QueueResult(queueIds[index], fromSeqs[index]);
        }
        return r;
    }

    boolean hasCapacity(int size) {
        return count == 0 || count < limit && size <= bytesLimit - bytes;
    }

    void add(long pos, int size, RaftTask task) {
        if (lengths == null) {
            int n = Math.min(limit, 16);
            lengths = new int[n];
            cached = new RefBuffer[n];
        } else if (count == lengths.length) {
            int n = (int) Math.min(limit, (long) count * 2);
            if (positions != null) {
                positions = Arrays.copyOf(positions, n);
            }
            lengths = Arrays.copyOf(lengths, n);
            cached = Arrays.copyOf(cached, n);
        }
        if (task != null) {
            RefBuffer ref = task.reqData.buffer;
            ref.retain();
            cached[count] = ref;
            if (size >= RaftServerConfig.GATHERING_WRITE_THRESHOLD) {
                cachedZeroCopyBytes += size;
            }
        } else {
            if (positions == null) {
                positions = new long[lengths.length];
            }
            positions[count] = pos;
        }
        lengths[count++] = size;
        bytes += size;
    }

    private FrameCallResult prepareResponse() {
        int headerSize = MqPullResp.headerSize(queueIds, fromSeqs, results, bytes);
        bodySize = Math.addExact(headerSize, bytes);
        int bufferSize = bodySize - cachedZeroCopyBytes;
        buffer = groupConfig.fiberGroup.dispatcher.thread.buffers.borrowDirect(bufferSize);
        ByteBuffer dest = buffer.getBuffer();
        dest.limit(bufferSize);
        MqPullResp.writeHeader(dest, queueIds, fromSeqs, results, bytes);
        ArrayList<ByteBuffer> segs = null;
        int[] offsets = null;
        int segmentStart = 0;
        for (int q = 0; count > 0 && q < results.length; q++) {
            QueueResult r = results[q];
            if (r == null) {
                continue;
            }
            // Collection is memory-first; the wire format remains grouped in request order.
            for (int n = 0, i = r.memoryStart; n < r.count; n++, i++) {
                if (n == r.memoryCount) {
                    i = r.ioStart;
                }
                int size = lengths[i];
                RefBuffer ref = cached[i];
                if (ref == null) {
                    if (offsets == null) {
                        offsets = new int[count];
                    }
                    offsets[i] = dest.position();
                    dest.position(dest.position() + size);
                } else {
                    ByteBuffer src = ref.getBuffer();
                    if (size >= RaftServerConfig.GATHERING_WRITE_THRESHOLD) {
                        if (segs == null) {
                            segs = new ArrayList<>(2);
                        }
                        addSegment(dest, segmentStart, segs);
                        segmentStart = dest.position();
                        segs.add(src.duplicate().clear().limit(size));
                    } else {
                        // Apply and replication share the source cursor; leave it unchanged.
                        if (src.hasArray()) {
                            dest.put(src.array(), src.arrayOffset(), size);
                        } else {
                            dest.put(src.duplicate().clear().limit(size));
                        }
                        ref.release();
                        cached[i] = null;
                    }
                }
            }
        }
        if (segs == null) {
            dest.flip();
        } else {
            addSegment(dest, segmentStart, segs);
            segments = segs;
        }
        if (offsets == null) {
            return afterRead(true);
        }
        int readCount = 0;
        for (int i = 0; i < count; i++) {
            // Every disk offset follows the header; zero denotes an in-memory record.
            if (offsets[i] != 0) {
                positions[readCount] = positions[i];
                lengths[readCount] = lengths[i];
                offsets[readCount++] = offsets[i];
            }
        }
        return Fiber.call(raftLog.read(positions, lengths, offsets, readCount, dest), this::afterRead);
    }

    private void addSegment(ByteBuffer dest, int start, ArrayList<ByteBuffer> segs) {
        if (dest.position() > start) {
            ByteBuffer view = dest.duplicate();
            view.flip().position(start);
            segs.add(view);
        }
    }

    private boolean resetPositionsValid() {
        if (results == null) {
            return true;
        }
        long first = -1;
        for (QueueResult r : results) {
            if (r != null && r.truncated) {
                if (first < 0) {
                    first = raftLog.firstReadablePos();
                }
                if (r.resetPos < first
                        || (r.resetPos == Long.MAX_VALUE && manager.queues.get(r.queueId).nextSeq > r.fromSeq)) {
                    return false;
                }
            }
        }
        return true;
    }

    private FrameCallResult afterRead(boolean success) {
        checkCancelled();
        if (!success || !resetPositionsValid()) {
            return Fiber.resume(null, this::scan);
        }
        MqPullResp result = new MqPullResp(buffer, cached, segments, bodySize);
        buffer = null;
        cached = null;
        segments = null;
        return justReturn(result);
    }

    private void watch() {
        if (waiting) {
            return;
        }
        waiting = true;
        // registration stays until doFinally; signals missed during scans are covered
        // by the changeVersion re-check
        changed = groupConfig.fiberGroup.newCondition("mqPull");
        for (long id : queueIds) {
            HashSet<MqPullFrame> set = mq.waitingPulls.get(id);
            if (set == null) {
                set = new HashSet<>();
                mq.waitingPulls.put(id, set);
            }
            set.add(this);
        }
    }

    private void unwatch() {
        if (!waiting) {
            return;
        }
        waiting = false;
        for (long id : queueIds) {
            HashSet<MqPullFrame> set = mq.waitingPulls.get(id);
            set.remove(this);
            if (set.isEmpty()) {
                mq.waitingPulls.remove(id);
            }
        }
    }

    private void releaseBuffers() {
        if (buffer != null) {
            buffer.release();
            buffer = null;
        }
        if (cached != null) {
            for (int i = 0; i < count; i++) {
                RefBuffer ref = cached[i];
                if (ref != null) {
                    ref.release();
                    cached[i] = null;
                }
            }
        }
        segments = null;
    }

    @Override
    protected FrameCallResult doFinally() {
        unwatch();
        mq.activePulls.remove(this);
        releaseBuffers();
        return Fiber.frameReturn();
    }
}
