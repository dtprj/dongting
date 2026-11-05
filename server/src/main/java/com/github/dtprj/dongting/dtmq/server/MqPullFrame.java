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
import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.net.DtChannel;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftTimeoutException;
import com.github.dtprj.dongting.raft.impl.RaftCancelException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Returns a pre-encoded response; the caller sends it or calls clean(). */
public final class MqPullFrame extends FiberFrame<MqPullRespPacket> {
    /** Truncated queues return only a retry cursor. The next cursor is fromSeq + queueCount. */
    public static final class QueueResult {
        public final long queueId;
        final long requestedSeq;
        public long fromSeq;
        public int queueCount;
        public boolean truncated;
        // pos of the item at fromSeq, or Long.MAX_VALUE if the seek lands on endSeq
        long resetPos;
        static final byte NONE = 0, BLOCK_MISS = 1, TAIL_MISS = 2;
        // why the memory scan stopped: BLOCK_MISS goes to the idx reader, TAIL_MISS to phase 2a
        byte stopReason;
        // the queue's items occupy [start, start + queueCount) in the flat arrays; phase 2a/2b re-anchor it
        int start;

        private QueueResult(long queueId, long requestedSeq) {
            this.queueId = queueId;
            this.requestedSeq = requestedSeq;
            reset();
        }

        private void reset() {
            fromSeq = requestedSeq;
            queueCount = 0;
            truncated = false;
            resetPos = Long.MAX_VALUE;
            stopReason = NONE;
            start = 0;
        }
    }

    final DtMQ mq;
    final long[] queueIds;
    final long[] fromSeqs;
    // created lazily, one entry per scanned queue; null entry means idle and encodes from fromSeqs
    QueueResult[] results;
    DtChannel channel;
    final int limit;
    final int bytesLimit;
    private final long waitNanos;
    private final DtTime deadline;

    private boolean cancelled;
    // absolute end of this long poll; 0 until the first scan anchors it
    private long waitEnd;
    private FiberCondition changed;
    // mq.changeVersion snapshot; a mismatch after a scan means appends landed during it
    private long changeVersion;

    // flat item arrays shared by all queues; msgPositions is allocated lazily, disk items only
    private long[] msgPositions;
    private int[] msgSizes;
    private RefBuffer[] cached;
    private int count;
    private int bytes;
    private int bodySize;
    private RefBuffer buffer;
    // gathering-write segments for large zero-copy items
    private List<ByteBuffer> segments;

    MqPullFrame(DtMQ mq, long[] queueIds, long[] fromSeqs, int limit, int bytesLimit,
                long waitMillis, DtTime deadline) {
        if (queueIds.length == 0 || queueIds.length != fromSeqs.length) {
            throw new IllegalArgumentException("queueIds and fromSeqs must have the same nonzero length");
        }
        DtUtil.checkPositive(limit, "limit");
        DtUtil.checkPositive(bytesLimit, "bytesLimit");
        if (waitMillis < 0 || waitMillis > Long.MAX_VALUE / 1_000_000) {
            throw new IllegalArgumentException("invalid waitMillis: " + waitMillis);
        }
        this.mq = mq;
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
        mq.activePulls.add(this);
        return scan(null);
    }

    private boolean isCancelled() {
        RaftGroupConfigEx gc = mq.groupConfig;
        boolean channelClosed = channel != null && !channel.getChannel().isOpen();
        if (Thread.currentThread() != gc.fiberGroup.dispatcher.thread) {
            return channelClosed || gc.fiberGroup.shareStatusSource.getShareStatus(true).shouldStop;
        }
        RaftStatusImpl rs = mq.raftStatus;
        MqIdxManager m = mq.manager;
        // the manager is swapped only inside DtMQ.startInstall(), which sets installing first
        return channelClosed || cancelled || rs.isShouldStop() || rs.installSnapshot || mq.installing
                || m == null || m.markClose;
    }

    void checkCancelled() {
        if (isCancelled()) {
            throw new RaftCancelException("mq pull cancelled");
        }
        if (deadline.isTimeout(mq.raftStatus.ts)) {
            throw new RaftTimeoutException("mq pull timeout");
        }
    }

    private FrameCallResult scan(Void unused) {
        releaseBuffers();
        checkCancelled();
        if (waitEnd == 0) {
            // fresh anchor; a stale one would shorten the wait
            mq.groupConfig.ts.refresh(1);
            waitEnd = mq.groupConfig.ts.nanoTime + waitNanos;
        }
        changeVersion = mq.changeVersion;
        count = 0;
        bytes = 0;
        QueueResult[] results = this.results;
        if (results != null) {
            for (QueueResult r : results) {
                if (r != null) {
                    r.reset();
                }
            }
        }
        LongObjMap<MqIdxQueue> queues = mq.manager.queues;
        for (int i = 0; i < queueIds.length && hasCapacity(); i++) {
            MqIdxQueue queue = queues.get(queueIds[i]);
            if (queue == null || fromSeqs[i] >= queue.nextSeq) {
                continue;
            }
            if (results == null) {
                results = new QueueResult[queueIds.length];
                this.results = results;
            }
            QueueResult r = results[i];
            if (r == null) {
                r = results[i] = new QueueResult(queueIds[i], fromSeqs[i]);
            }
            r.start = count;
            MqIdxReader.scanMemory(this, queue, r);
        }
        // phase 2a: fill the remaining budget with TAIL_MISS queues before any idx reader
        for (int i = 0; results != null && i < results.length && hasCapacity(); i++) {
            QueueResult r = results[i];
            if (r != null && r.stopReason == QueueResult.TAIL_MISS) {
                // hits form a seq suffix: phase 2a/2b only run for queues that collected nothing
                if (r.queueCount != 0) {
                    BugLog.log("mq pull tail miss after collected records: queue={}, fromSeq={}, queueCount={}",
                            r.queueId, r.fromSeq, r.queueCount);
                } else {
                    r.start = count;
                    MqIdxReader.continueScan(this, queues.get(r.queueId), r);
                }
            }
        }
        return readQueue(0);
    }

    private FrameCallResult readQueue(int idx) {
        checkCancelled();
        while (results != null && idx < results.length && hasCapacity()) {
            QueueResult r = results[idx];
            if (r != null && r.stopReason == QueueResult.BLOCK_MISS) {
                // same invariant as the phase 2a guard
                if (r.queueCount != 0) {
                    BugLog.log("mq pull block miss after collected records: queue={}, fromSeq={}, queueCount={}",
                            r.queueId, r.fromSeq, r.queueCount);
                } else {
                    r.start = count;
                    int next = idx + 1;
                    // a false return means stale state mid-collect: the whole scan restarts
                    return Fiber.call(new MqIdxReader(this, mq.manager.queues.get(r.queueId), r),
                            v -> Boolean.TRUE.equals(v) ? readQueue(next) : Fiber.resume(null, this::scan));
                }
            }
            idx++;
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
        long rest = waitEnd - mq.groupConfig.ts.nanoTime;
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
        return changed.await(rest, TimeUnit.NANOSECONDS, mq.groupConfig.fiberGroup.shouldStopCondition, this::scan);
    }

    // soft byte limit: any single item may cross it; collection stops once the total reaches it
    boolean hasCapacity() {
        return count < limit && bytes < bytesLimit;
    }

    private static boolean zeroCopy(RefBuffer ref, int size) {
        return ref != null && size >= RaftServerConfig.GATHERING_WRITE_THRESHOLD;
    }

    void add(long pos, int size, RaftTask task) {
        if (msgSizes == null) {
            int n = Math.min(limit, 16);
            msgSizes = new int[n];
            cached = new RefBuffer[n];
        } else if (count == msgSizes.length) {
            int n = (int) Math.min(limit, (long) count * 2);
            if (msgPositions != null) {
                msgPositions = Arrays.copyOf(msgPositions, n);
            }
            msgSizes = Arrays.copyOf(msgSizes, n);
            cached = Arrays.copyOf(cached, n);
        }
        if (task != null) {
            RefBuffer ref = task.reqData.buffer;
            ref.retain();
            cached[count] = ref;
        } else {
            if (msgPositions == null) {
                msgPositions = new long[msgSizes.length];
            }
            msgPositions[count] = pos;
            cached[count] = null;
        }
        msgSizes[count++] = size;
        bytes += size;
    }

    private FrameCallResult prepareResponse() {
        int headerSize = MqPullRespPacket.headerSize(queueIds, fromSeqs, results, bytes);
        bodySize = Math.addExact(headerSize, bytes);
        // zero-copy bytes are excluded from the borrowed response buffer size
        int zeroCopyBytes = 0;
        for (int i = 0; i < count; i++) {
            if (zeroCopy(cached[i], msgSizes[i])) {
                zeroCopyBytes += msgSizes[i];
            }
        }
        int bufferSize = bodySize - zeroCopyBytes;
        buffer = mq.groupConfig.fiberGroup.dispatcher.thread.buffers.borrowDirect(bufferSize);
        ByteBuffer dest = buffer.getBuffer();
        dest.limit(bufferSize);
        MqPullRespPacket.writeHeader(dest, queueIds, fromSeqs, results, bytes);
        ArrayList<ByteBuffer> segs = null;
        int[] offsets = null;
        int segmentStart = 0;
        for (int q = 0; count > 0 && q < results.length; q++) {
            QueueResult r = results[q];
            if (r == null) {
                continue;
            }
            // tail-cache hits form a seq suffix: one contiguous range per queue, emitted in request order
            for (int i = r.start, end = i + r.queueCount; i < end; i++) {
                int size = msgSizes[i];
                RefBuffer ref = cached[i];
                if (ref == null) {
                    if (offsets == null) {
                        offsets = new int[count];
                    }
                    offsets[i] = dest.position();
                    dest.position(dest.position() + size);
                } else {
                    ByteBuffer src = ref.getBuffer();
                    if (zeroCopy(ref, size)) {
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
                msgPositions[readCount] = msgPositions[i];
                msgSizes[readCount] = msgSizes[i];
                offsets[readCount++] = offsets[i];
            }
        }
        return Fiber.call(mq.raftLog.read(msgPositions, msgSizes, offsets, readCount, dest, this::isCancelled),
                this::afterRead);
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
                    first = mq.raftLog.seekFloor();
                }
                if (r.resetPos < first
                        || (r.resetPos == Long.MAX_VALUE && mq.manager.queues.get(r.queueId).nextSeq > r.fromSeq)) {
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
        MqPullRespPacket result = new MqPullRespPacket(buffer, cached, segments, bodySize);
        buffer = null;
        cached = null;
        segments = null;
        return justReturn(result);
    }

    private void watch() {
        if (changed != null) {
            return;
        }
        // registration stays until doFinally; signals missed during scans are covered
        // by the changeVersion re-check
        changed = mq.groupConfig.fiberGroup.newCondition("mqPull");
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
        if (changed == null) {
            return;
        }
        for (long id : queueIds) {
            HashSet<MqPullFrame> set = mq.waitingPulls.get(id);
            set.remove(this);
            if (set.isEmpty()) {
                mq.waitingPulls.remove(id);
            }
        }
        changed = null;
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
