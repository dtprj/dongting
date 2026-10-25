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
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftTimeoutException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.store.RaftLog;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

/** Returns concatenated raw log records in queue order; null means no records, and the caller releases the buffer. */
public final class MqPullFrame extends FiberFrame<RefBuffer> {
    /** Truncated queues return only a retry cursor; requiredBytes identifies a record exceeding bytesLimit. */
    public static final class QueueResult {
        public final long queueId;
        final long requestedSeq;
        public long fromSeq;
        public long nextSeq;
        public int count;
        public boolean truncated;
        public int requiredBytes;
        long resetPos;

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
            requiredBytes = 0;
            resetPos = Long.MAX_VALUE;
        }
    }

    private final DtMQ mq;
    final RaftGroupConfigEx groupConfig;
    final RaftStatusImpl raftStatus;
    final RaftLog raftLog;
    private final RaftGroup raftGroup;
    private final QueueResult[] results;
    final int limit;
    final int bytesLimit;
    private final long waitNanos;
    private final DtTime deadline;
    private final FiberCondition changed;
    MqIdxManager manager;
    private boolean cancelled;
    private boolean waiting;
    private long waitEnd;
    private long changeVersion;
    private int queueIndex;
    private long[] positions;
    private int[] lengths;
    int count;
    private int bytes;
    boolean retry;
    private RefBuffer buffer;

    MqPullFrame(DtMQ mq, RaftGroupConfigEx groupConfig, RaftGroup raftGroup, RaftLog raftLog,
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
        this.raftGroup = raftGroup;
        this.raftLog = raftLog;
        this.limit = limit;
        this.bytesLimit = bytesLimit;
        this.waitNanos = TimeUnit.MILLISECONDS.toNanos(waitMillis);
        this.deadline = Objects.requireNonNull(deadline);
        this.changed = groupConfig.fiberGroup.newCondition("mqPull");
        this.results = new QueueResult[queueIds.length];
        HashSet<Long> ids = new HashSet<>();
        for (int i = 0; i < queueIds.length; i++) {
            if (fromSeqs[i] < 0 || !ids.add(queueIds[i])) {
                throw new IllegalArgumentException("negative fromSeq or duplicate queueId");
            }
            results[i] = new QueueResult(queueIds[i], fromSeqs[i]);
        }
    }

    public int getQueueCount() {
        return results.length;
    }

    public QueueResult getQueueResult(int index) {
        return results[index];
    }

    /** Raft thread only; does not interrupt in-flight I/O or release its buffers prematurely. */
    public void cancel() {
        cancelled = true;
        changed.signalAll();
    }

    void signal() {
        changed.signalAll();
    }

    @Override
    public FrameCallResult execute(Void input) {
        if (getFiber().isDaemon()) {
            throw new RaftException("mq pull should not run in daemon fiber");
        }
        manager = mq.manager;
        mq.activePulls.add(this);
        waitEnd = System.nanoTime() + waitNanos;
        return scan(null);
    }

    void checkCancelled() {
        if (cancelled || isGroupShouldStopPlain() || raftStatus.installSnapshot
                || manager == null || manager.markClose || manager != mq.manager) {
            throw new CancellationException("mq pull cancelled");
        }
        if (deadline.isTimeout()) {
            throw new RaftTimeoutException("mq pull timeout");
        }
    }

    private void checkLease() {
        checkCancelled();
        if (!raftGroup.isLeaseReadValid(groupConfig.ts, deadline)) {
            throw new RaftException("raft group is not ready");
        }
    }

    private FrameCallResult scan(Void unused) {
        unwatch();
        checkLease();
        changeVersion = mq.changeVersion;
        queueIndex = 0;
        count = 0;
        bytes = 0;
        retry = false;
        for (QueueResult r : results) {
            r.reset();
        }
        return readQueue(null);
    }

    private FrameCallResult readQueue(Void unused) {
        checkCancelled();
        if (retry) {
            return Fiber.resume(null, this::scan);
        }
        while (queueIndex < results.length) {
            QueueResult r = results[queueIndex++];
            MqIdxQueue q = manager.get(r.queueId);
            if (q != null) {
                return Fiber.call(new MqIdxReader(this, q, r), this::readQueue);
            }
        }
        checkLease();
        if (!resetPositionsValid()) {
            return Fiber.resume(null, this::scan);
        }
        if (count > 0) {
            buffer = groupConfig.fiberGroup.dispatcher.thread.buffers.borrowDirect(bytes);
            buffer.getBuffer().limit(bytes);
            return Fiber.call(raftLog.read(positions, lengths, count, buffer.getBuffer()), this::afterRead);
        }
        for (QueueResult r : results) {
            if (r.truncated || r.requiredBytes != 0) {
                return justReturn(null);
            }
        }
        long rest = waitEnd - System.nanoTime();
        if (waitNanos == 0 || rest <= 0) {
            return justReturn(null);
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

    boolean add(long pos, int size) {
        if (count == limit || size > bytesLimit - bytes) {
            return false;
        }
        if (positions == null) {
            int n = Math.min(limit, 16);
            positions = new long[n];
            lengths = new int[n];
        } else if (count == positions.length) {
            int n = (int) Math.min(limit, (long) count * 2);
            positions = Arrays.copyOf(positions, n);
            lengths = Arrays.copyOf(lengths, n);
        }
        positions[count] = pos;
        lengths[count++] = size;
        bytes += size;
        return true;
    }

    private boolean resetPositionsValid() {
        long first = raftLog.firstReadablePos();
        for (QueueResult r : results) {
            if (r.truncated && (r.resetPos < first
                    || (r.resetPos == Long.MAX_VALUE && manager.get(r.queueId).nextSeq > r.fromSeq))) {
                return false;
            }
        }
        return true;
    }

    private FrameCallResult afterRead(boolean success) {
        checkLease();
        if (!success || !resetPositionsValid()) {
            buffer.release();
            buffer = null;
            return Fiber.resume(null, this::scan);
        }
        buffer.getBuffer().flip();
        RefBuffer result = buffer;
        buffer = null;
        return justReturn(result);
    }

    private void watch() {
        waiting = true;
        for (QueueResult r : results) {
            HashSet<MqPullFrame> set = mq.waitingPulls.get(r.queueId);
            if (set == null) {
                set = new HashSet<>();
                mq.waitingPulls.put(r.queueId, set);
            }
            set.add(this);
        }
    }

    private void unwatch() {
        if (!waiting) {
            return;
        }
        waiting = false;
        for (QueueResult r : results) {
            HashSet<MqPullFrame> set = mq.waitingPulls.get(r.queueId);
            set.remove(this);
            if (set.isEmpty()) {
                mq.waitingPulls.remove(r.queueId);
            }
        }
    }

    @Override
    protected FrameCallResult doFinally() {
        unwatch();
        mq.activePulls.remove(this);
        if (buffer != null) {
            buffer.release();
            buffer = null;
        }
        return Fiber.frameReturn();
    }
}
