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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.buf.Buffers;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.fiber.DispatcherThread;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class ChainWriter {
    private static final DtLog log = DtLogs.getLogger(ChainWriter.class);

    private final PerfCallback perfCallback;
    private final RaftGroupConfigEx config;
    private final Consumer<WriteTask> writeCallback;
    private final Consumer<WriteTask> forceCallback;
    private final RaftStatusImpl raftStatus;

    private int writePerfType1;
    private int writePerfType2;
    private int forcePerfType;

    private final Buffers buffers;
    private final LinkedList<WriteTask> writeTasks = new LinkedList<>();
    private final LinkedList<WriteTask> forceTasks = new LinkedList<>();

    private final FiberCondition needForceCondition;
    private final Fiber forceFiber;

    private boolean error;

    private int writeTaskCount;
    private int forceTaskCount;

    private boolean markStop;

    public ChainWriter(String fiberNamePrefix, RaftGroupConfigEx config, Consumer<WriteTask> writeCallback,
                       Consumer<WriteTask> forceCallback) {
        this.config = config;
        this.perfCallback = config.perfCallback;
        this.writeCallback = writeCallback;
        this.forceCallback = forceCallback;
        this.raftStatus = (RaftStatusImpl) config.raftStatus;

        DispatcherThread t = config.fiberGroup.dispatcher.thread;
        this.buffers = t.buffers;
        this.needForceCondition = config.fiberGroup.newCondition("needForceCond");
        this.forceFiber = new Fiber(fiberNamePrefix + "-" + config.groupId, config.fiberGroup,
                new ForceLoopFrame());
    }

    public void start() {
        forceFiber.start();
    }

    private boolean shouldCancelRetry() {
        return error || raftStatus.installSnapshot;
    }

    public FiberFuture<Void> stop() {
        this.markStop = true;
        needForceCondition.signal();
        if (forceFiber.isStarted()) {
            return forceFiber.join();
        } else {
            return FiberFuture.completedFuture(config.fiberGroup, null);
        }
    }

    public static class WriteTask {
        private final AsyncIoTask ioTask;
        private final long posInFile;
        private final long expectNextPos;
        private final boolean force;
        private final ByteBuffer buf;

        private final int perfWriteItemCount;
        private final int perfWriteBytes;
        private final long lastRaftIndex;

        private int perfForceItemCount;
        private long perfForceBytes;


        public WriteTask(FiberGroup fiberGroup, LogFile logFile, int[] retryInterval, boolean retryForever,
                         Supplier<Boolean> cancelIndicator, ByteBuffer buf, long posInFile, boolean force,
                         int perfItemCount, long lastRaftIndex) {
            this.ioTask = new AsyncIoTask(fiberGroup, logFile, retryInterval, retryForever, cancelIndicator);
            this.posInFile = posInFile;
            this.force = force;
            this.buf = buf;
            this.perfWriteItemCount = perfItemCount;
            int remaining = buf == null ? 0 : buf.remaining();
            this.perfWriteBytes = remaining;
            this.expectNextPos = posInFile + remaining;
            this.lastRaftIndex = lastRaftIndex;
        }

        public void submitWrite() {
            if (buf != null && buf.remaining() > 0) {
                ioTask.write(buf, posInFile);
            } else {
                ioTask.getFuture().complete(null);
            }
        }

        public long getLastRaftIndex() {
            return lastRaftIndex;
        }

        public LogFile getLogFile() {
            return (LogFile) ioTask.getDtFile();
        }

        public FiberFuture<Void> getFuture() {
            return ioTask.getFuture();
        }
    }

    public void submitWrite(LogFile logFile, boolean initialized, ByteBuffer buf, long posInFile, boolean force,
                            int perfItemCount, long lastRaftIndex) {
        if (error) {
            log.warn("in error state, ignore write");
            return;
        }
        int[] retryInterval = initialized ? config.ioRetryInterval : null;
        WriteTask task = new WriteTask(config.fiberGroup, logFile, retryInterval, true,
                this::shouldCancelRetry, buf, posInFile, force, perfItemCount, lastRaftIndex);
        if (!writeTasks.isEmpty()) {
            WriteTask lastTask = writeTasks.getLast();
            if (lastTask.getLogFile() == task.getLogFile()) {
                if (lastTask.expectNextPos != task.posInFile) {
                    throw Fiber.fatal(new RaftException("pos not continuous"));
                }
            }
        }
        long startTime = perfCallback.takeTimeAndRefresh(writePerfType2, config.ts);
        FiberFuture<Void> f = task.getFuture();
        logFile.incWriters();
        try {
            task.submitWrite();
        } catch (Throwable e) {
            logFile.decWriters();
            throw e;
        }
        if (writePerfType1 > 0) {
            perfCallback.fireTime(writePerfType1, startTime, task.perfWriteItemCount, task.perfWriteBytes);
        }
        writeTaskCount++;
        writeTasks.add(task);
        f.registerCallback((v, ex) -> afterWrite(ex, task, startTime));
    }

    private void afterWrite(Throwable ioEx, WriteTask task, long startTime) {
        perfCallback.fireTimeAndRefresh(writePerfType2, startTime, task.perfWriteItemCount, task.perfWriteBytes, config.ts);
        if (task.buf != null) {
            buffers.release(task.buf);
        }
        writeTaskCount--;
        if (error || raftStatus.installSnapshot) {
            task.getLogFile().decWriters();
            return;
        }
        if (ioEx != null) {
            log.error("write file {} error: {}", task.getLogFile().getFile(), ioEx.toString());
            error = true;
            task.getLogFile().decWriters();
            FiberGroup.currentGroup().requestShutdown();
            return;
        }
        LinkedList<WriteTask> writeTasks = this.writeTasks;
        WriteTask lastTaskNeedCallback = null;
        while (!writeTasks.isEmpty()) {
            WriteTask t = writeTasks.getFirst();
            FiberFuture<Void> f = t.getFuture();
            if (f.isDone()) {
                writeTasks.removeFirst();
                lastTaskNeedCallback = t;
                if (t.force) {
                    forceTasks.add(t);
                    forceTaskCount++;
                } else {
                    t.getLogFile().decWriters();
                }
            } else {
                break;
            }
        }
        if (lastTaskNeedCallback != null) {
            needForceCondition.signal();
            if (writeCallback != null) {
                writeCallback.accept(lastTaskNeedCallback);
            }
        }
    }

    private class ForceLoopFrame extends FiberFrame<Void> {
        private WriteTask currentForceTask;

        @Override
        protected FrameCallResult handle(Throwable ex) {
            error = true;
            if (currentForceTask != null) {
                currentForceTask.getLogFile().decWriters();
                currentForceTask = null;
            }
            cleanPendingTasks();
            if (raftStatus.installSnapshot) {
                log.info("install snapshot, force fiber exit: {}", forceFiber.name, ex);
                return Fiber.frameReturn();
            } else {
                throw Fiber.fatal(ex);
            }
        }

        private void cleanPendingTasks() {
            for (WriteTask ft : forceTasks) {
                ft.getLogFile().decWriters();
            }
            forceTasks.clear();
            forceTaskCount = 0;
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (error || raftStatus.installSnapshot) {
                log.info("force fiber exit: {}", forceFiber.name);
                cleanPendingTasks();
                return Fiber.frameReturn();
            }
            if (markStop && writeTaskCount <= 0 && forceTaskCount <= 0) {
                log.debug("force fiber exit normally: {}", forceFiber.name);
                return Fiber.frameReturn();
            }
            LinkedList<WriteTask> forceTasks = ChainWriter.this.forceTasks;
            if (forceTasks.isEmpty()) {
                return needForceCondition.await(this);
            } else {
                WriteTask task = forceTasks.removeFirst();
                task.perfForceItemCount = task.perfWriteItemCount;
                task.perfForceBytes = task.perfWriteBytes;
                WriteTask nextTask;
                while ((nextTask = forceTasks.peekFirst()) != null) {
                    if (task.getLogFile() == nextTask.getLogFile()) {
                        nextTask.perfForceItemCount = nextTask.perfWriteItemCount + task.perfForceItemCount;
                        nextTask.perfForceBytes = nextTask.perfWriteBytes + task.perfForceBytes;
                        task.getLogFile().decWriters();
                        task = nextTask;
                        forceTasks.removeFirst();
                        forceTaskCount--;
                    } else {
                        break;
                    }
                }
                LogFile logFile = task.getLogFile();
                if (logFile.shouldDelete() || logFile.deleted) {
                    log.warn("file {} should delete or deleted, ignore force", logFile.getFile());
                    forceTaskCount--;
                    logFile.decWriters();
                    forceCallback.accept(task);
                    return Fiber.resume(null, this);
                }
                ForceFrame ff = new ForceFrame(logFile, config.blockIoExecutor, false);
                RetryFrame<Void> rf = new RetryFrame<>(ff, config.ioRetryInterval,
                        true, ChainWriter.this::shouldCancelRetry);
                WriteTask finalTask = task;
                long perfStartTime = perfCallback.takeTimeAndRefresh(forcePerfType, config.ts);
                currentForceTask = task;
                return Fiber.call(rf, v -> afterForce(finalTask, perfStartTime));
            }
        }

        private FrameCallResult afterForce(WriteTask task, long perfStartTime) {
            currentForceTask = null;
            perfCallback.fireTimeAndRefresh(forcePerfType, perfStartTime, task.perfForceItemCount, task.perfForceBytes, config.ts);
            forceTaskCount--;

            if (error || raftStatus.installSnapshot) {
                task.getLogFile().decWriters();
                cleanPendingTasks();
                return Fiber.frameReturn();
            }

            task.getLogFile().decWriters();
            forceCallback.accept(task);
            return Fiber.resume(null, this);
        }
    }

    public void setWritePerfType1(int writePerfType1) {
        this.writePerfType1 = writePerfType1;
    }

    public void setWritePerfType2(int writePerfType2) {
        this.writePerfType2 = writePerfType2;
    }

    public void setForcePerfType(int forcePerfType) {
        this.forcePerfType = forcePerfType;
    }
}
