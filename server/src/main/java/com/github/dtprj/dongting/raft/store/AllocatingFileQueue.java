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

import com.github.dtprj.dongting.common.PerfConsts;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;

/**
 * A FileQueue that allocates the next file ahead of time in a dedicated fiber,
 * so writers do not block on file creation when they approach the queue end.
 *
 * @author huangli
 */
abstract class AllocatingFileQueue extends FileQueue {
    private static final DtLog log = DtLogs.getLogger(AllocatingFileQueue.class);

    private final Fiber queueAllocFiber;
    private final FiberCondition needAllocCond;
    private final FiberCondition allocDoneCond;
    private long allocPos = -1;
    private boolean stopAlloc;

    public AllocatingFileQueue(File dir, RaftGroupConfigEx groupConfig, long fileSize, boolean mainLogFile) {
        super(dir, groupConfig, fileSize, mainLogFile);
        this.needAllocCond = groupConfig.fiberGroup.newCondition("needAllocCond");
        this.allocDoneCond = groupConfig.fiberGroup.newCondition("allocDoneCond");
        this.queueAllocFiber = new Fiber("queueAlloc" + groupConfig.groupId,
                groupConfig.fiberGroup, new QueueAllocFrame());
    }

    protected void startQueueAllocFiber() {
        queueAllocFiber.start();
    }

    @Override
    protected FiberFuture<Void> stopFileQueue() {
        stopAlloc = true;
        needAllocCond.signal();
        return queueAllocFiber.join().compose("waitNoRwAndClose", v -> super.stopFileQueue());
    }

    protected void tryAllocateAsync(long pos) {
        if (pos > allocPos) {
            allocPos = pos;
            if (pos > queueEndPosition - fileSize) {
                needAllocCond.signalAll();
            }
        }
    }

    protected boolean isWritePosReady(long pos) {
        tryAllocateAsync(pos);
        return pos < queueEndPosition;
    }

    protected FiberFrame<Void> ensureWritePosReady(long pos) {
        return new FiberFrame<>() {
            boolean block;
            long blockPerfStartTime;

            @Override
            public FrameCallResult execute(Void input) {
                tryAllocateAsync(pos);
                int perfType = mainLogFile ? PerfConsts.RAFT_D_LOG_POS_NOT_READY : PerfConsts.RAFT_D_IDX_POS_NOT_READY;
                if (pos >= queueEndPosition) {
                    if (!block) {
                        block = true;
                        blockPerfStartTime = groupConfig.perfCallback.takeTimeAndRefresh(perfType, groupConfig.ts);
                    }
                    if (queueAllocFiber.isFinished()) {
                        throw new RaftException("ensureWritePosReady " + pos + " failed because queueAllocFiber is finished");
                    }
                    return allocDoneCond.await(5000, this);
                } else {
                    if (block) {
                        groupConfig.perfCallback.fireTimeAndRefresh(perfType, blockPerfStartTime, 1, 0, groupConfig.ts);
                    }
                    return Fiber.frameReturn();
                }
            }
        };
    }

    private class QueueAllocFrame extends FiberFrame<Void> {

        @Override
        public FrameCallResult execute(Void input) {
            if (raftStatus.installSnapshot || stopAlloc) {
                allocDoneCond.signalAll();
                log.info("{} queue alloc fiber exit", mainLogFile ? "log" : "idx");
                return Fiber.frameReturn();
            }
            // pre-allocate when allocPos enters the last file, so the next file is ready
            // before writes actually reach the end of the current queue
            if (allocPos > queueEndPosition - fileSize) {
                FileAllocFrame f = new FileAllocFrame();
                RetryFrame<Void> rf = new RetryFrame<>(f, groupConfig.ioRetryInterval, true,
                        () -> raftStatus.installSnapshot || stopAlloc);
                return Fiber.call(rf, v -> afterAlloc(f));
            } else {
                return needAllocCond.await(5000, this);
            }
        }

        private FrameCallResult afterAlloc(FileAllocFrame f) {
            LogFile logFile = f.logFile;
            lruAddLast(logFile);
            queue.addLast(logFile);
            if (queue.size() == 1) {
                queueStartPosition = logFile.startPos;
            }
            queueEndPosition = logFile.endPos;
            allocDoneCond.signalAll();
            return Fiber.resume(null, this);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            allocDoneCond.signalAll();
            if (raftStatus.installSnapshot || stopAlloc) {
                log.info("{} queue alloc fiber exit on cancel", mainLogFile ? "log" : "idx");
                return Fiber.frameReturn();
            }
            throw Fiber.fatal(ex);
        }
    }

    private class FileAllocFrame extends FiberFrame<Void> {
        private long fileStartPos;

        private File file;
        private LogFile logFile;
        private final int perfType;
        private long perfStartTime;

        public FileAllocFrame() {
            this.perfType = mainLogFile ? PerfConsts.RAFT_D_LOG_FILE_ALLOC : PerfConsts.RAFT_D_IDX_FILE_ALLOC;
        }

        @Override
        public FrameCallResult execute(Void v) {
            perfStartTime = groupConfig.perfCallback.takeTime(perfType);
            logFile = null;
            fileStartPos = queueEndPosition;
            String fileName = String.format("%020d", fileStartPos);
            file = new File(dir, fileName);
            FiberFuture<Void> createFileFuture = getFiberGroup().newFuture("createFile");
            ioExecutor.execute(() -> {
                long startTime = System.currentTimeMillis();
                try {
                    RandomAccessFile raf = new RandomAccessFile(file, "rw");
                    raf.setLength(getFileSize());
                    raf.getFD().sync();
                    raf.close();
                    Set<OpenOption> options = Set.of(StandardOpenOption.READ, StandardOpenOption.WRITE);
                    logFile = new LogFile(fileStartPos, fileStartPos + getFileSize(), file,
                            groupConfig.fiberGroup, options, ioExecutor, AllocatingFileQueue.this::lruTouch,
                            raftStatus.ts.wallClockMillis, mainLogFile);
                    // access in io thread, but happens-before use
                    logFile.syncOpen();
                    long time = System.currentTimeMillis() - startTime;
                    createFileFuture.fireComplete(null);
                    log.info("allocate file done, cost {} ms: {}", time, file.getPath());
                } catch (Throwable e) {
                    long time = System.currentTimeMillis() - startTime;
                    createFileFuture.fireCompleteExceptionally(e);
                    log.info("allocate file failed, cost {} ms: {}", time, file, e);
                }
            });
            return createFileFuture.await(this::afterCreateFile);
        }

        private FrameCallResult afterCreateFile(Void unused) {
            groupConfig.perfCallback.fireTime(perfType, perfStartTime);
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult handle(Throwable ex) throws Throwable {
            if (logFile != null) {
                logFile.destroy();
            }
            throw ex;
        }
    }
}
