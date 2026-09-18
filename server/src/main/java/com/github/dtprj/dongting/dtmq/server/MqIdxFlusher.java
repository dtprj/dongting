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

import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.FutureFrame;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;
import com.github.dtprj.dongting.raft.store.LogFile;
import com.github.dtprj.dongting.raft.store.RetryFrame;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * Drives mq idx flush. Dispatcher thread only: io futures complete via fireComplete, so
 * registered callbacks run in the dispatcher thread directly. A flush round is a serial
 * chain with exactly one io in flight, so activeRounds doubles as the pending io count.
 *
 * @author huangli
 */
class MqIdxFlusher {

    private static final DtLog log = DtLogs.getLogger(MqIdxFlusher.class);

    private final MqIdxManager manager;
    private final RaftGroupConfigEx groupConfig;
    private final RaftStatusImpl raftStatus;

    private final Fiber loopFiber;
    private final FiberCondition requestCond;
    private final FiberCondition roundDoneCond;
    private final FiberCondition allocRetryCond;
    private final ArrayList<Pair<Long, FiberFuture<Void>>> waiters = new ArrayList<>();

    private FiberFuture<Void> closeFuture;
    private int activeRounds;
    private int flushAllTargetCount;
    private long requestVersion;
    private long finishedVersion;
    private boolean cleanupRetry;

    private boolean error;

    // queues with a pending threshold request; drained by the loop fiber
    private final IndexedQueue<QueueIdxInfo> roundRequests = new IndexedQueue<>(16);

    // shared by FlushAllRoundFrame and AllQueuesCleanupFrame; filled and drained per round
    private final IndexedQueue<QueueIdxInfo> todo = new IndexedQueue<>(64);

    private final Supplier<Boolean> cancelRetryIndicator;

    MqIdxFlusher(MqIdxManager manager) {
        this.manager = manager;
        this.cancelRetryIndicator = () -> manager.markClose || error;
        this.groupConfig = manager.groupConfig;
        this.raftStatus = (RaftStatusImpl) groupConfig.raftStatus;
        this.loopFiber = new Fiber("mqIdxFlushLoop-" + groupConfig.groupId,
                groupConfig.fiberGroup, new IdxLoopFrame());
        this.requestCond = groupConfig.fiberGroup.newCondition("mqIdxFlushRequest");
        this.roundDoneCond = groupConfig.fiberGroup.newCondition("mqIdxRoundDone");
        this.allocRetryCond = groupConfig.fiberGroup.newCondition("mqIdxAllocRetry");
    }

    void start() {
        loopFiber.start();
    }

    FiberFrame<Void> createCleanupFrame() {
        return new AllQueuesCleanupFrame(todo);
    }

    private class AllQueuesCleanupFrame extends FiberFrame<Void> {
        private final IndexedQueue<QueueIdxInfo> todo;

        AllQueuesCleanupFrame(IndexedQueue<QueueIdxInfo> todo) {
            this.todo = todo;
            manager.queues.forEach((LongObjMap.ReadOnlyVisitor<QueueIdxInfo>) (id, q) -> todo.addLast(q));
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (error || manager.markClose) {
                return Fiber.frameReturn();
            }
            while (true) {
                QueueIdxInfo q = todo.pollFirst();
                if (q == null) {
                    return Fiber.frameReturn();
                }
                // in-memory check: most queues need no io at all
                if (q.needRunCleanup(raftStatus.firstValidPos)) {
                    return Fiber.call(q.createCleanupFrame(), v -> afterCleanup(q));
                }
            }
        }

        private FrameCallResult afterCleanup(QueueIdxInfo q) {
            if (q.lastCleanupFailed) {
                cleanupRetry = true;
            }
            return Fiber.resume(null, this);
        }
    }

    // dispatcher thread; the loop fiber is the only round starter, so a cleanup deleting
    // files cannot interleave with a round start
    void requestRound(QueueIdxInfo q) {
        if (roundWanted(q) && !q.roundRequested) {
            q.roundRequested = true;
            roundRequests.addLast(q);
            requestCond.signal();
        }
    }

    private boolean roundWanted(QueueIdxInfo q) {
        return q.nextSeq - 1 - q.writeFinishSeq >= groupConfig.mqIdxFlushThreshold;
    }

    private void startRound(QueueIdxInfo q, boolean force, long targetSeq) {
        if (error || manager.markClose || q.flushing) {
            return;
        }
        q.flushing = true;
        q.flushForce = force;
        q.flushTargetSeq = targetSeq;
        activeRounds++;
        continueRound(q);
    }

    FiberFuture<Void> flushAll() {
        FiberFuture<Void> f = groupConfig.fiberGroup.newFuture("mqIdxFlushAll");
        if (error || manager.markClose || !loopFiber.isStarted() || loopFiber.isFinished()) {
            f.fireCompleteExceptionally(new RaftException("mq idx flusher is not running"));
        } else {
            requestVersion++;
            requestCond.signal();
            waiters.add(new Pair<>(requestVersion, f));
        }
        return f;
    }

    private void finishWaiters(long version) {
        Iterator<Pair<Long, FiberFuture<Void>>> it = waiters.iterator();
        while (it.hasNext()) {
            Pair<Long, FiberFuture<Void>> w = it.next();
            if (w.getLeft() <= version) {
                it.remove();
                w.getRight().fireComplete(null);
            }
        }
    }

    private void giveUpWaiters(String msg) {
        for (Pair<Long, FiberFuture<Void>> w : waiters) {
            w.getRight().fireCompleteExceptionally(new RaftException(msg));
        }
        waiters.clear();
    }

    /**
     * Must be idempotent.
     */
    FiberFuture<Void> close() {
        if (closeFuture != null) {
            return closeFuture;
        }
        manager.markClose = true;
        manager.completeBlockFuture();
        requestCond.signal();
        roundDoneCond.signalAll();
        allocRetryCond.signalAll();
        giveUpWaiters("mq idx flusher is closing");
        closeFuture = FutureFrame.startWaitFiber("mqIdxClose-" + groupConfig.groupId,
                groupConfig.fiberGroup, new CloseFrame());
        return closeFuture;
    }

    private void continueRound(QueueIdxInfo q) {
        if (error || manager.markClose || !roundIncomplete(q)) {
            endRound(q);
            return;
        }
        if (q.writeFinishSeq < q.flushTargetSeq) {
            if (q.needAllocateFile()) {
                submitFileAlloc(q);
            } else {
                submitWrite(q);
            }
        } else {
            LogFile lf = q.currentWriteFile();
            if (lf == null) {
                BugLog.log("current write file not found: queue=" + q.queueId
                        + ", writeFinishSeq=" + q.writeFinishSeq);
                endRound(q);
                return;
            }
            submitForce(q, lf, q.writeFinishSeq);
        }
    }

    private boolean roundIncomplete(QueueIdxInfo q) {
        return q.writeFinishSeq < q.flushTargetSeq
                || (q.flushForce && q.forceFinishSeq < q.writeFinishSeq);
    }

    private void endRound(QueueIdxInfo q) {
        q.flushing = false;
        activeRounds--;
        retireTarget(q);
        roundDoneCond.signalAll();
        requestRound(q);
    }

    private boolean retireTarget(QueueIdxInfo q) {
        if (q.flushAllTarget && q.forceFinishSeq >= q.flushTargetSeq) {
            q.flushAllTarget = false;
            flushAllTargetCount--;
            return true;
        }
        return false;
    }

    private void submitWrite(QueueIdxInfo q) {
        QueueIdxInfo.FlushBatch b = q.prepareBatch();
        b.logFile.incWriters();
        try {
            AsyncIoTask ioTask = new AsyncIoTask(groupConfig.fiberGroup, b.logFile,
                    groupConfig.ioRetryInterval, cancelRetryIndicator);
            FiberFuture<Void> f = b.force
                    ? ioTask.writeAndForce(b.bufRef.getBuffer(), b.filePos)
                    : ioTask.write(b.bufRef.getBuffer(), b.filePos);
            f.registerCallback((v, ex) -> onIoDone(q, b, ex));
        } catch (Throwable t) {
            b.bufRef.release();
            b.logFile.decWriters();
            throw t;
        }
    }

    private void submitForce(QueueIdxInfo q, LogFile logFile, long endSeq) {
        logFile.incWriters();
        QueueIdxInfo.FlushBatch b = new QueueIdxInfo.FlushBatch(endSeq, null, logFile, -1, true);
        try {
            AsyncIoTask ioTask = new AsyncIoTask(groupConfig.fiberGroup, logFile,
                    groupConfig.ioRetryInterval, cancelRetryIndicator);
            ioTask.force().registerCallback((v, ex) -> onIoDone(q, b, ex));
        } catch (Throwable t) {
            logFile.decWriters();
            throw t;
        }
    }

    private void onIoDone(QueueIdxInfo q, QueueIdxInfo.FlushBatch b, Throwable ex) {
        try {
            b.logFile.decWriters();
            if (b.bufRef != null) {
                b.bufRef.release();
            }
            if (ex != null) {
                endRound(q);
                if (cancelRetryIndicator.get()) {
                    // retry is canceled by close or a previous failure, so the error is expected
                    log.warn("give up mq idx io: queue={}, file={}", q.queueId,
                            b.logFile.getFile().getPath(), ex);
                } else {
                    // retry budget exhausted
                    error = true;
                    log.error("mq idx io fail after retries, shutdown group: queue={}, file={}",
                            q.queueId, b.logFile.getFile().getPath(), ex);
                    FiberGroup.currentGroup().requestShutdown();
                }
                return;
            }
            if (b.bufRef != null) {
                q.writeFinishSeq = b.endSeq;
                manager.evict();
            }
            if (b.force) {
                q.forceFinishSeq = b.endSeq;
            }
            continueRound(q);
        } catch (Throwable t) {
            throw Fiber.fatal(t);
        }
    }

    private void submitFileAlloc(QueueIdxInfo q) {
        RetryFrame<LogFile> rf = new RetryFrame<>(new AllocAttemptFrame(q),
                groupConfig.ioRetryInterval, cancelRetryIndicator);
        rf.cancelCondition = allocRetryCond;
        FiberFuture<LogFile> f = FutureFrame.startWaitFiber(
                "mqIdxFileAlloc-" + groupConfig.groupId + "-" + q.queueId, groupConfig.fiberGroup, rf);
        f.registerCallback((lf, ex) -> onAllocated(q, lf, ex));
    }

    private void onAllocated(QueueIdxInfo q, LogFile lf, Throwable ex) {
        try {
            if (ex == null) {
                if (error || manager.markClose) {
                    // don't attach to the file queue on close or error; the file on disk is removed
                    // by the following destroy, or re-extended by a later allocation
                    lf.destroy();
                    endRound(q);
                    return;
                }
                q.attachFile(lf, q.nextWriteFileStartPos());
                continueRound(q);
            } else {
                endRound(q);
                if (cancelRetryIndicator.get()) {
                    // retry is canceled by close or a previous failure, so the error is expected
                    log.warn("give up mq idx file allocation: queue={}", q.queueId, ex);
                } else {
                    // retry budget exhausted
                    error = true;
                    log.error("mq idx file allocation fail after retries, shutdown group: queue={}",
                            q.queueId, ex);
                    FiberGroup.currentGroup().requestShutdown();
                }
            }
        } catch (Throwable t) {
            throw Fiber.fatal(t);
        }
    }

    private class AllocAttemptFrame extends FiberFrame<LogFile> {
        private final QueueIdxInfo q;

        AllocAttemptFrame(QueueIdxInfo q) {
            this.q = q;
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (manager.markClose) {
                throw new RaftException("mq idx flusher is closing");
            }
            long fileStart = q.nextWriteFileStartPos();
            File file = q.createFileByStartPos(fileStart);
            FiberFuture<LogFile> f = groupConfig.fiberGroup.newFuture("mqIdxFileAlloc");
            try {
                groupConfig.blockIoExecutor.execute(() -> {
                    try {
                        f.fireComplete(allocateFile(q, file, fileStart));
                    } catch (Throwable t) {
                        log.error("allocate mq idx file failed: {}", file.getPath(), t);
                        f.fireCompleteExceptionally(t);
                    }
                });
            } catch (Throwable t) {
                f.completeExceptionally(t);
            }
            return f.await(this::justReturn);
        }

        private LogFile allocateFile(QueueIdxInfo q, File file, long fileStart) throws IOException {
            File parent = file.getParentFile();
            if (parent != null && !parent.isDirectory()
                    && !parent.mkdirs() && !parent.isDirectory()) {
                throw new IOException("create queue dir fail: " + parent.getPath());
            }
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.setLength(q.getFileSize());
                raf.getFD().sync();
            }
            LogFile lf = new LogFile(fileStart, fileStart + q.getFileSize(), file,
                    groupConfig.fiberGroup, groupConfig.blockIoExecutor,
                    q::lruTouch, System.currentTimeMillis(), false);
            lf.syncOpen();
            return lf;
        }
    }

    private class CloseFrame extends FiberFrame<Void> {
        private ArrayList<QueueIdxInfo> qs;
        private int index = -1;

        @Override
        public FrameCallResult execute(Void input) {
            if (loopFiber.isStarted() && !loopFiber.isFinished()) {
                return loopFiber.join().await(this);
            }
            if (activeRounds > 0) {
                return roundDoneCond.await(1000, this);
            }
            if (qs == null) {
                qs = new ArrayList<>(manager.queues.size());
                manager.queues.forEach((id, q) -> {
                    qs.add(q);
                });
            }
            index++;
            if (index >= qs.size()) {
                log.info("mq idx flusher closed, groupId={}", groupConfig.groupId);
                return Fiber.frameReturn();
            }
            return qs.get(index).close().await(this);
        }
    }

    private class IdxLoopFrame extends FiberFrame<Void> {

        // -1: run a cleanup on the first tick, so restart leftovers are judged immediately
        private long lastCleanupFirstValidPos = -1;
        private boolean pendingCleanup;
        private long lastFlushTickNanos = raftStatus.ts.nanoTime;

        @Override
        public FrameCallResult execute(Void input) {
            if (error || manager.markClose) {
                giveUpWaiters("mq idx flusher is not running");
                log.info("mq idx flush loop exit, groupId={}", groupConfig.groupId);
                return Fiber.frameReturn();
            }
            // threshold rounds first: they are the latency-sensitive path
            processRoundRequests();
            long now = groupConfig.ts.nanoTime;
            if (now - lastFlushTickNanos >= groupConfig.mqIdxFlushIntervalMillis * 1_000_000L) {
                lastFlushTickNanos = now;
                if (raftStatus.firstValidPos != lastCleanupFirstValidPos || cleanupRetry) {
                    lastCleanupFirstValidPos = raftStatus.firstValidPos;
                    cleanupRetry = false;
                    pendingCleanup = true;
                }
                requestVersion++;
                manager.queues.forEach((id, q) -> {
                    q.closeIdleFiles();
                });
                return Fiber.yield(this);
            }
            if (requestVersion > finishedVersion) {
                return Fiber.call(new FlushAllRoundFrame(), this);
            }
            if (pendingCleanup) {
                // cleanup only touches head files, so it goes after flush-all rounds
                pendingCleanup = false;
                return Fiber.call(createCleanupFrame(), this);
            }
            return requestCond.await(groupConfig.mqIdxFlushIntervalMillis, this);
        }

        private void processRoundRequests() {
            QueueIdxInfo q;
            while ((q = roundRequests.pollFirst()) != null) {
                q.roundRequested = false;
                if (roundWanted(q)) {
                    startRound(q, false, q.nextSeq - 1);
                }
            }
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            giveUpWaiters("mq idx flush-all loop error");
            throw Fiber.fatal(ex);
        }
    }

    private class FlushAllRoundFrame extends FiberFrame<Void> {
        private final long version;

        FlushAllRoundFrame() {
            this.version = requestVersion;
            flushAllTargetCount = 0;
            // nextSeq never decreases, so anchoring may only raise flushTargetSeq of a round
            // still in flight; the flush-all guarantee (force up to the round-start watermark) always holds
            manager.queues.forEach((queueId, q) -> {
                if (q.isDirty()) {
                    q.flushTargetSeq = q.nextSeq - 1;
                    q.flushAllTarget = true;
                    flushAllTargetCount++;
                    todo.addLast(q);
                }
            });
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (error) {
                giveUpWaiters("mq idx flush fail");
                return Fiber.frameReturn();
            }
            if (manager.markClose) {
                return Fiber.frameReturn();
            }
            drainRequests();
            QueueIdxInfo q;
            while ((q = todo.pollFirst()) != null) {
                if (retireTarget(q)) {
                    continue;
                }
                if (q.flushing) {
                    // upgrade the running write-only round, it forces up to the target before ending
                    q.flushForce = true;
                    continue;
                }
                if (activeRounds >= groupConfig.mqIdxFlushAllConcurrency) {
                    todo.addFirst(q);
                    break;
                }
                startRound(q, true, q.flushTargetSeq);
            }
            if (flushAllTargetCount == 0 && todo.size() == 0) {
                finishedVersion = version;
                finishWaiters(version);
                return Fiber.frameReturn();
            }
            return roundDoneCond.await(1000, this);
        }

        private void drainRequests() {
            QueueIdxInfo q;
            while ((q = roundRequests.pollFirst()) != null) {
                q.roundRequested = false;
                if (!retireTarget(q) && q.flushAllTarget) {
                    continue;
                }
                q.flushTargetSeq = q.nextSeq - 1;
                todo.addLast(q);
            }
        }
    }
}
