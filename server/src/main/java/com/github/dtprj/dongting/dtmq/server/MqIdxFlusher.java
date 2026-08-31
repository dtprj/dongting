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

    private final Fiber flushAllFiber;
    private final FiberCondition requestCond;
    private final FiberCondition roundCond;
    private final FiberCondition allocRetryCond;
    private final ArrayList<Pair<Long, FiberFuture<Void>>> waiters = new ArrayList<>();

    private FiberFuture<Void> closeFuture;
    private int activeRounds;
    private int forceRounds;
    private long requestVersion;
    private long finishedVersion;
    private long lastTickNanos;

    private boolean error;

    private final Supplier<Boolean> cancelRetryIndicator;

    MqIdxFlusher(MqIdxManager manager) {
        this.manager = manager;
        this.cancelRetryIndicator = () -> manager.markClose || error;
        this.groupConfig = manager.groupConfig;
        this.flushAllFiber = new Fiber("mqIdxFlushAll-" + groupConfig.groupId,
                groupConfig.fiberGroup, new FlushLoopFrame());
        this.requestCond = groupConfig.fiberGroup.newCondition("mqIdxFlushRequest");
        this.roundCond = groupConfig.fiberGroup.newCondition("mqIdxFlushRound");
        this.allocRetryCond = groupConfig.fiberGroup.newCondition("mqIdxAllocRetry");
        this.lastTickNanos = groupConfig.ts.nanoTime;
    }

    void start() {
        flushAllFiber.start();
    }

    void maybeStartRound(QueueIdxInfo q) {
        if (q.nextSeq - 1 - q.writeFinishSeq >= groupConfig.mqIdxFlushThreshold) {
            startRound(q, false, q.nextSeq - 1);
        }
    }

    void startRound(QueueIdxInfo q, boolean force, long targetSeq) {
        if (error || manager.markClose || q.flushing) {
            return;
        }
        q.flushing = true;
        q.flushForce = force;
        q.flushTargetSeq = targetSeq;
        activeRounds++;
        if (force) {
            forceRounds++;
        }
        continueRound(q);
    }

    FiberFuture<Void> flushAll() {
        FiberFuture<Void> f = groupConfig.fiberGroup.newFuture("mqIdxFlushAll");
        if (error || manager.markClose || !flushAllFiber.isStarted()) {
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
        roundCond.signalAll();
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
        if (q.flushForce) {
            forceRounds--;
        }
        roundCond.signalAll();
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
            if (flushAllFiber.isStarted() && !flushAllFiber.isFinished()) {
                return flushAllFiber.join().await(this);
            }
            if (activeRounds > 0) {
                return roundCond.await(1000, this);
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
            return qs.get(index).closeFiles().await(this);
        }
    }

    private class FlushLoopFrame extends FiberFrame<Void> {

        private final IndexedQueue<QueueIdxInfo> todo = new IndexedQueue<>(64);

        @Override
        public FrameCallResult execute(Void input) {
            if (error || manager.markClose) {
                giveUpWaiters("mq idx flusher is not running");
                log.info("mq idx flush-all fiber exit, groupId={}", groupConfig.groupId);
                return Fiber.frameReturn();
            }
            long now = groupConfig.ts.nanoTime;
            if (now - lastTickNanos >= groupConfig.mqIdxFlushIntervalMillis * 1_000_000L) {
                lastTickNanos = now;
                requestVersion++;
                manager.queues.forEach((id, q) -> {
                    q.closeIdleFiles();
                });
                return Fiber.yield(this);
            }
            if (requestVersion > finishedVersion) {
                return Fiber.call(new FlushAllRoundFrame(todo), this);
            }
            return requestCond.await(groupConfig.mqIdxFlushIntervalMillis, this);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            giveUpWaiters("mq idx flush-all loop error");
            throw Fiber.fatal(ex);
        }
    }

    private class FlushAllRoundFrame extends FiberFrame<Void> {
        private final long version;
        private final IndexedQueue<QueueIdxInfo> todo;

        FlushAllRoundFrame(IndexedQueue<QueueIdxInfo> todo) {
            this.version = requestVersion;
            this.todo = todo;
            // nextSeq never decreases, so a later trigger round may only raise flushTargetSeq;
            // the flush-all guarantee (force up to the round-start watermark) always holds
            manager.queues.forEach((queueId, q) -> {
                if (q.isDirty()) {
                    q.flushTargetSeq = q.nextSeq - 1;
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
            QueueIdxInfo q;
            while ((q = todo.pollFirst()) != null) {
                if (q.forceFinishSeq >= q.flushTargetSeq) {
                    continue;
                }
                if (q.flushing) {
                    // upgrade the running trigger round, it forces up to the target before ending
                    q.flushForce = true;
                    forceRounds++;
                    continue;
                }
                if (activeRounds >= groupConfig.mqIdxFlushAllConcurrency) {
                    todo.addFirst(q);
                    break;
                }
                startRound(q, true, q.flushTargetSeq);
            }
            if (todo.size() == 0 && forceRounds == 0) {
                finishedVersion = version;
                finishWaiters(version);
                return Fiber.frameReturn();
            }
            return roundCond.await(1000, this);
        }
    }
}
