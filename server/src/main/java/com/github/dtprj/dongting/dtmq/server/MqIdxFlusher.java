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

import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.FutureFrame;
import com.github.dtprj.dongting.fiber.SimpleFrame;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;
import com.github.dtprj.dongting.raft.store.RetryFrame;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * Drives mq idx flush and cleanup in a single loop fiber. Dispatcher thread only: io
 * futures complete via fireComplete, so registered callbacks run in the dispatcher
 * thread directly. A flush round is a serial chain with exactly one io in flight; all
 * rounds are capped by mqIdxFlushAllConcurrency. Every loop pass serves flush before
 * cleanup; the cleanup walk runs per clean interval, skips files held by in-flight
 * io and catches the rest with the next cycle.
 *
 * @author huangli
 */
class MqIdxFlusher {

    private static final DtLog log = DtLogs.getLogger(MqIdxFlusher.class);

    long cleanIntervalMillis = 60_000;

    private final MqIdxManager manager;
    private final RaftGroupConfigEx groupConfig;
    private final RaftStatusImpl raftStatus;

    private final Fiber loopFiber;
    private final FiberCondition roundCond;
    private final FiberCondition cancelAllocRetryCond;
    private final ArrayList<Pair<Long, FiberFuture<Void>>> flushAllWaiters = new ArrayList<>();

    private FiberFuture<Void> stopFuture;
    private int activeRounds;
    private int flushAllTargetCount;
    private long requestVersion;
    private long finishedVersion;

    private boolean error;

    // reused across rounds
    private final ArrayDeque<MqIdxQueue> flushAllQueue = new ArrayDeque<>(128);
    private final ArrayDeque<MqIdxQueue> flushQueue = new ArrayDeque<>(128);

    private final Supplier<Boolean> cancelRetryIndicator;

    MqIdxFlusher(MqIdxManager manager) {
        this.manager = manager;
        this.cancelRetryIndicator = () -> manager.markClose || error;
        this.groupConfig = manager.groupConfig;
        this.raftStatus = (RaftStatusImpl) groupConfig.raftStatus;
        this.loopFiber = new Fiber("mqIdxFlushLoop-" + groupConfig.groupId,
                groupConfig.fiberGroup, new IdxLoopFrame());
        this.roundCond = groupConfig.fiberGroup.newCondition("mqIdxRound");
        this.cancelAllocRetryCond = groupConfig.fiberGroup.newCondition("mqIdxCancelAllocRetry");
    }

    void start() {
        loopFiber.start();
    }

    void requestRound(MqIdxQueue q) {
        if (!q.flushing && !q.flushQueued && pendingReachesThreshold(q)) {
            q.flushQueued = true;
            flushQueue.addLast(q);
            roundCond.signalAll();
        }
    }

    private boolean pendingReachesThreshold(MqIdxQueue q) {
        return q.nextSeq - 1 - q.writeFinishSeq >= groupConfig.mqIdxFlushThreshold;
    }

    private void startRound(MqIdxQueue q, boolean force, long targetSeq) {
        if (error || manager.markClose || q.flushing) {
            return;
        }
        q.flushing = true;
        q.flushForce = force;
        q.flushTargetSeq = targetSeq;
        activeRounds++;
        continueRound(q);
    }

    private void continueRound(MqIdxQueue q) {
        if (error || manager.markClose) {
            endRound(q);
            return;
        }
        if (q.writeFinishSeq < q.flushTargetSeq) {
            if (q.needAllocateFile()) {
                submitFileAlloc(q);
            } else {
                submitWrite(q);
            }
        } else if (q.flushForce && q.forceFinishSeq < q.writeFinishSeq) {
            MqIdxFile lf = q.currentWriteFile();
            if (lf == null) {
                BugLog.log("current write file not found: queue=" + q.queueId
                        + ", writeFinishSeq=" + q.writeFinishSeq);
                endRound(q);
                return;
            }
            submitForce(q, lf, q.writeFinishSeq);
        } else {
            endRound(q);
        }
    }

    private void endRound(MqIdxQueue q) {
        q.flushing = false;
        activeRounds--;
        retireTarget(q);
        roundCond.signalAll();
        if (error || manager.markClose) {
            return;
        }
        requestRound(q);
    }

    FiberFuture<Void> flushAll() {
        FiberFuture<Void> f = groupConfig.fiberGroup.newFuture("mqIdxFlushAll");
        if (error || manager.markClose || !loopFiber.isStarted() || loopFiber.isFinished()) {
            f.fireCompleteExceptionally(new RaftException("mq idx flusher is not running"));
        } else {
            requestVersion++;
            flushAllWaiters.add(new Pair<>(requestVersion, f));
            roundCond.signalAll();
        }
        return f;
    }

    private void finishWaiters(long version) {
        Iterator<Pair<Long, FiberFuture<Void>>> it = flushAllWaiters.iterator();
        while (it.hasNext()) {
            Pair<Long, FiberFuture<Void>> w = it.next();
            if (w.getLeft() <= version) {
                it.remove();
                w.getRight().fireComplete(null);
            }
        }
    }

    private void giveUpWaiters(String msg) {
        for (Pair<Long, FiberFuture<Void>> w : flushAllWaiters) {
            w.getRight().fireCompleteExceptionally(new RaftException(msg));
        }
        flushAllWaiters.clear();
    }

    /**
     * Must be idempotent. The caller must have set manager.markClose before.
     */
    FiberFuture<Void> stop() {
        if (stopFuture != null) {
            return stopFuture;
        }
        roundCond.signalAll();
        cancelAllocRetryCond.signalAll();
        giveUpWaiters("mq idx flusher is stopping");
        String stopFiberName = "mqIdxFlusherStop-" + groupConfig.groupId;
        stopFuture = FutureFrame.startWaitFiber(stopFiberName,
                groupConfig.fiberGroup, new SimpleFrame<>(stopFiberName, this::mqIdxFlusherStop));
        return stopFuture;
    }

    private FrameCallResult mqIdxFlusherStop(SimpleFrame<Void> frame) {
        if (!error) {
            if (loopFiber.isStarted() && !loopFiber.isFinished()) {
                return loopFiber.join().await(frame);
            }
            if (activeRounds > 0) {
                return roundCond.await(1000, frame);
            }
        }
        log.info("mq idx flusher stopped, groupId={}", groupConfig.groupId);
        return Fiber.frameReturn();
    }

    private boolean retireTarget(MqIdxQueue q) {
        if (q.flushAllTarget >= 0 && q.forceFinishSeq >= q.flushAllTarget) {
            q.flushAllTarget = -1;
            flushAllTargetCount--;
            return true;
        }
        return false;
    }

    private void submitWrite(MqIdxQueue q) {
        MqIdxQueue.FlushBatch b = q.prepareBatch();
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

    private void submitForce(MqIdxQueue q, MqIdxFile logFile, long endSeq) {
        logFile.incWriters();
        MqIdxQueue.FlushBatch b = new MqIdxQueue.FlushBatch(endSeq, null, logFile, -1, true, -1);
        try {
            AsyncIoTask ioTask = new AsyncIoTask(groupConfig.fiberGroup, logFile,
                    groupConfig.ioRetryInterval, cancelRetryIndicator);
            ioTask.force().registerCallback((v, ex) -> onIoDone(q, b, ex));
        } catch (Throwable t) {
            logFile.decWriters();
            throw t;
        }
    }

    private void onIoDone(MqIdxQueue q, MqIdxQueue.FlushBatch b, Throwable ex) {
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
                if (b.lastItemPos != -1) {
                    // the batch seals the file: its last item pos is now frozen
                    b.logFile.lastItemPos = b.lastItemPos;
                }
                manager.evict();
            }
            if (b.force) {
                q.forceFinishSeq = b.endSeq;
                if (retireTarget(q)) {
                    // the anchored target is met: the round may keep flushing newer data
                    roundCond.signalAll();
                }
            }
            continueRound(q);
        } catch (Throwable t) {
            error = true;
            throw Fiber.fatal(t);
        }
    }

    private void submitFileAlloc(MqIdxQueue q) {
        SimpleFrame<MqIdxFile> allocFrame = new SimpleFrame<>("allocAttempt",
                frame -> frameAllocAttempt(frame, q));
        RetryFrame<MqIdxFile> rf = new RetryFrame<>(allocFrame, groupConfig.ioRetryInterval, cancelRetryIndicator);
        rf.cancelCondition = cancelAllocRetryCond;
        FiberFuture<MqIdxFile> f = FutureFrame.startWaitFiber(
                "mqIdxFileAlloc-" + groupConfig.groupId + "-" + q.queueId, groupConfig.fiberGroup, rf);
        f.registerCallback((lf, ex) -> onAllocated(q, lf, ex));
    }

    private void onAllocated(MqIdxQueue q, MqIdxFile lf, Throwable ex) {
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
            error = true;
            throw Fiber.fatal(t);
        }
    }


    private FrameCallResult frameAllocAttempt(SimpleFrame<MqIdxFile> frame, MqIdxQueue q) {
        if (manager.markClose) {
            throw new RaftException("mq idx flusher is closing");
        }
        long fileStart = q.nextWriteFileStartPos();
        File file = q.createFileByStartPos(fileStart);
        FiberFuture<MqIdxFile> f = groupConfig.fiberGroup.newFuture("mqIdxFileAlloc");
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
        return f.await(frame::justReturn);
    }

    private MqIdxFile allocateFile(MqIdxQueue q, File file, long fileStart) throws IOException {
        File parent = file.getParentFile();
        if (parent != null && !parent.isDirectory()
                && !parent.mkdirs() && !parent.isDirectory()) {
            throw new IOException("create queue dir fail: " + parent.getPath());
        }
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(q.getFileSize());
            raf.getFD().sync();
        }
        MqIdxFile lf = q.createFile(file, fileStart, System.currentTimeMillis());
        lf.syncOpen();
        return lf;
    }

    private class IdxLoopFrame extends FiberFrame<Void> {

        private long lastFlushTickNanos = raftStatus.ts.nanoTime;
        private long lastCleanStartNanos = raftStatus.ts.nanoTime;
        private final ArrayDeque<MqIdxQueue> cleanQueue = new ArrayDeque<>(128);
        private long roundVersion = -1;

        @Override
        public FrameCallResult execute(Void input) {
            if (error || manager.markClose) {
                giveUpWaiters("mq idx flusher is not running");
                log.info("mq idx flush loop exit, groupId={}", groupConfig.groupId);
                return Fiber.frameReturn();
            }
            finishFlushAllRound();
            long now = groupConfig.ts.nanoTime;
            boolean needStartFlush = roundVersion == -1 && ((now - lastFlushTickNanos >=
                    groupConfig.mqIdxFlushIntervalMillis * 1_000_000L) || requestVersion > finishedVersion);
            boolean needStartClean = now - lastCleanStartNanos >= cleanIntervalMillis
                    * 1_000_000L && cleanQueue.isEmpty();
            if (needStartFlush || needStartClean) {
                if (needStartFlush) {
                    lastFlushTickNanos = now;
                    roundVersion = requestVersion;
                }
                if (needStartClean) {
                    lastCleanStartNanos = now;
                }
                manager.queues.forEach((queueId, q) -> {
                    if (needStartFlush) {
                        if (q.isDirty()) {
                            q.flushAllTarget = q.nextSeq - 1;
                            flushAllTargetCount++;
                            if (!q.flushQueued) {
                                flushAllQueue.addLast(q);
                                q.flushQueued = true;
                            }
                        }
                    }
                    if (needStartClean) {
                        cleanQueue.addLast(q);
                    }
                });
                if (manager.queues.size() > 1000) {
                    return Fiber.yield(this);
                }
            }
            // always process flush first
            while (activeRounds < groupConfig.mqIdxFlushAllConcurrency) {
                MqIdxQueue q = flushQueue.pollFirst();
                if (q == null) {
                    q = flushAllQueue.pollFirst();
                }
                if (q == null) {
                    break;
                }
                q.flushQueued = false;
                if (q.flushAllTarget >= 0) {
                    if (q.flushing) {
                        // upgrade the running round
                        q.flushForce = true;
                        if (q.flushTargetSeq < q.flushAllTarget) {
                            q.flushTargetSeq = q.flushAllTarget;
                        }
                    } else {
                        startRound(q, true, q.flushAllTarget);
                    }
                } else {
                    startRound(q, false, q.nextSeq - 1);
                }
            }
            finishFlushAllRound();
            for (int i = 0; i < 64 && !cleanQueue.isEmpty(); i++) {
                MqIdxQueue q = cleanQueue.pollFirst();
                q.closeIdleFiles();
                if (q.needRunCleanup(raftStatus.firstValidPos)) {
                    return Fiber.call(q.new CleanupFrame(), this);
                }
            }
            if (cleanQueue.isEmpty()) {
                long idleWaitMillis = Math.min(groupConfig.mqIdxFlushIntervalMillis,
                        cleanIntervalMillis);
                idleWaitMillis = Math.min(idleWaitMillis, 3000);
                return roundCond.await(idleWaitMillis, this);
            }
            return Fiber.yield(this);
        }

        private void finishFlushAllRound() {
            if (flushAllTargetCount == 0 && roundVersion != -1) {
                finishWaiters(roundVersion);
                finishedVersion = roundVersion;
                roundVersion = -1;
            }
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            error = true;
            giveUpWaiters("mq idx flush-all loop error");
            flushAllQueue.clear();
            flushQueue.clear();
            cleanQueue.clear();
            throw Fiber.fatal(ex);
        }
    }
}
