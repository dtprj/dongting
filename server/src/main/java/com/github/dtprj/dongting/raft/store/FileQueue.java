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

import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.PerfConsts;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.PostFiberFrame;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author huangli
 */
abstract class FileQueue {
    private static final DtLog log = DtLogs.getLogger(FileQueue.class);
    private static final Pattern PATTERN = Pattern.compile("^(\\d{20})$");
    protected final IndexedQueue<LogFile> queue = new IndexedQueue<>(32);
    protected final File dir;

    protected final ExecutorService ioExecutor;
    protected final RaftGroupConfigEx groupConfig;
    protected final RaftStatusImpl raftStatus;

    protected final long fileSize;
    protected final long fileLenMask;
    protected final int fileLenShiftBits;
    private final boolean mainLogFile;

    private final Fiber queueAllocFiber;
    private final FiberCondition needAllocCond;
    private final FiberCondition allocDoneCond;
    private long allocPos = -1;

    protected long queueStartPosition;
    protected long queueEndPosition;

    protected boolean initialized;

    protected boolean markClose;
    private boolean stopAlloc;

    public FileQueue(File dir, RaftGroupConfigEx groupConfig, long fileSize, boolean mainLogFile) {
        if (BitUtil.nextHighestPowerOfTwo(fileSize) != fileSize) {
            throw new IllegalArgumentException("fileSize not power of 2: " + fileSize);
        }
        this.dir = dir;
        this.ioExecutor = groupConfig.blockIoExecutor;
        this.groupConfig = groupConfig;
        this.raftStatus = (RaftStatusImpl) groupConfig.raftStatus;

        this.fileSize = fileSize;
        this.fileLenMask = fileSize - 1;
        this.fileLenShiftBits = BitUtil.zeroCountOfBinary(fileSize);
        this.mainLogFile = mainLogFile;

        this.needAllocCond = groupConfig.fiberGroup.newCondition("needAllocCond");
        this.allocDoneCond = groupConfig.fiberGroup.newCondition("allocDoneCond");
        this.queueAllocFiber = new Fiber("queueAlloc" + groupConfig.groupId,
                groupConfig.fiberGroup, new QueueAllocFrame(), true);
    }

    protected final long getFileSize() {
        return fileSize;
    }

    protected final long startPosOfFile(long pos) {
        return pos & (~fileLenMask);
    }

    protected void initQueue() throws IOException {
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return;
        }
        Arrays.sort(files);
        int count = 0;
        for (File f : files) {
            if (!f.isFile()) {
                continue;
            }
            Matcher matcher = PATTERN.matcher(f.getName());
            if (matcher.matches()) {
                if (f.length() != getFileSize()) {
                    throw new RaftException("file size error: " + f.getPath() + ", size=" + f.length());
                }
                long startPos = Long.parseLong(matcher.group(1));
                HashSet<OpenOption> openOptions = new HashSet<>();
                openOptions.add(StandardOpenOption.READ);
                openOptions.add(StandardOpenOption.WRITE);
                ExecutorService executor = groupConfig.ioCallbackUseGroupExecutor ?
                        groupConfig.fiberGroup.getExecutor() : ioExecutor;
                AsynchronousFileChannel channel = AsynchronousFileChannel.open(f.toPath(), openOptions, executor);
                queue.addLast(new LogFile(startPos, startPos + getFileSize(), channel, f, groupConfig.fiberGroup));
                count++;
            }
        }
        for (int i = 0; i < queue.size(); i++) {
            LogFile lf = queue.get(i);
            if ((lf.startPos & fileLenMask) != 0) {
                throw new RaftException("file start index error: " + lf.startPos);
            }
            if (i != 0 && lf.startPos != queue.get(i - 1).endPos) {
                throw new RaftException("not follow previous file " + lf.startPos);
            }
        }

        if (queue.size() > 0) {
            queueStartPosition = queue.get(0).startPos;
            queueEndPosition = queue.get(queue.size() - 1).endPos;
            log.info("load {} files in {}, first={}, last={}", count, dir.getPath(),
                    queue.get(0).getFile().getName(), queue.get(queue.size() - 1).getFile().getName());
        }
    }

    protected void startQueueAllocFiber() {
        queueAllocFiber.start();
    }

    protected FiberFuture<Void> stopFileQueue() {
        stopAlloc = true;
        needAllocCond.signal();
        FiberFuture<Void> f = groupConfig.fiberGroup.newFuture("fileQueueClose");
        queueAllocFiber.join().registerCallback((v, ex) -> {
            closeChannel();
            if (ex != null) {
                f.completeExceptionally(ex);
            } else {
                f.complete(null);
            }
        });
        return f;
    }

    // to delete all files that not be managed (unexpected)
    protected FiberFrame<Void> forceDeleteAll() {
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return FiberFrame.voidCompletedFrame();
        }
        return new FiberFrame<>() {
            int i = -1;

            @Override
            public FrameCallResult execute(Void input) {
                i++;
                if (i >= files.length) {
                    return Fiber.frameReturn();
                }
                File f = files[i];
                if (PATTERN.matcher(f.getName()).matches()) {
                    log.warn("delete unexpected file: {}", f.getPath());
                    return Fiber.call(new DeleteFrame(f, null), this);
                } else {
                    return Fiber.resume(null, this);
                }
            }
        };
    }

    protected LogFile getLogFile(long filePos) {
        if (filePos < queueStartPosition || filePos >= queueEndPosition) {
            return null;
        }
        int index = (int) ((filePos - queueStartPosition) >>> fileLenShiftBits);
        return queue.get(index);
    }

    protected void tryAllocateAsync(long pos) {
        if (pos > allocPos) {
            allocPos = pos;
            if (pos >= queueEndPosition - fileSize) {
                needAllocCond.signalAll();
            }
        }
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
                        blockPerfStartTime = groupConfig.perfCallback.takeTime(perfType);
                    }
                    if (queueAllocFiber.isFinished()) {
                        throw new RaftException("ensureWritePosReady " + pos + " failed because queueAllocFiber is finished");
                    }
                    return allocDoneCond.await(this);
                } else {
                    if (block) {
                        groupConfig.perfCallback.fireTime(perfType, blockPerfStartTime);
                    }
                    return Fiber.frameReturn();
                }
            }
        };
    }

    private void closeChannel() {
        for (int i = 0; i < queue.size(); i++) {
            DtUtil.close(queue.get(i).getChannel());
        }
    }

    private class DeleteFrame extends FiberFrame<Void> {

        private final File file;
        private final AsynchronousFileChannel channel;

        public DeleteFrame(File file, AsynchronousFileChannel channel) {
            this.file = file;
            this.channel = channel;
        }

        @Override
        public FrameCallResult execute(Void input) {
            FiberFuture<Void> deleteFuture = groupConfig.fiberGroup.newFuture("deleteFile");
            try {
                ioExecutor.execute(() -> {
                    try {
                        if (channel != null) {
                            log.debug("close log file: {}", file.getPath());
                            DtUtil.close(channel);
                        }
                        log.info("delete log file: {}", file.getPath());
                        Files.delete(file.toPath());

                        deleteFuture.fireComplete(null);
                    } catch (Throwable e) {
                        log.error("delete file fail: ", file.getPath());
                        deleteFuture.fireCompleteExceptionally(e);
                    }
                });
            } catch (Throwable e) {
                log.error("submit delete task fail: ", e);
                deleteFuture.completeExceptionally(e);
            }
            return deleteFuture.await(this::justReturn);
        }
    }

    public FiberFrame<Void> deleteFirstFile() {
        FiberFrame<Void> f = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                LogFile first = queue.get(0);
                if (first.inUse()) {
                    log.warn("file in use, wait. reader={}, writer={}, file={}", first.getReaders(),
                            first.getWriters(), first.getFile().getPath());
                    return first.getNoRwCond().await(this);
                }
                if (first.deleteTimestamp == 0) {
                    first.deleteTimestamp = 1;
                }
                first.deleted = true;
                return Fiber.call(new DeleteFrame(first.getFile(), first.getChannel()), this::justReturn);
            }
        };
        f = new RetryFrame<>(f, groupConfig.ioRetryInterval, true,
                () -> !initialized || raftStatus.installSnapshot);
        f = new PostFiberFrame<>(f) {
            @Override
            protected FrameCallResult postProcess(Void v) {
                queue.removeFirst();
                if (queue.size() >= 1) {
                    queueStartPosition = queue.get(0).startPos;
                } else {
                    queueStartPosition = 0;
                    queueEndPosition = 0;
                }
                FileQueue.this.afterDelete();
                return Fiber.frameReturn();
            }
        };
        return f;
    }

    protected void afterDelete() {
    }

    private class QueueAllocFrame extends FiberFrame<Void> {

        @Override
        public FrameCallResult execute(Void input) {
            if (raftStatus.installSnapshot || stopAlloc) {
                log.info("{} queue alloc fiber exit", FileQueue.this instanceof IdxFileQueue ? "idx" : "log");
                return Fiber.frameReturn();
            }
            if (allocPos >= queueEndPosition) {
                FileAllocFrame f = new FileAllocFrame();
                return Fiber.call(f, v -> afterAlloc(f));
            } else {
                return needAllocCond.await(1000, this);
            }
        }

        private FrameCallResult afterAlloc(FileAllocFrame f) {
            if (!f.result) {
                if (raftStatus.installSnapshot || stopAlloc) {
                    allocDoneCond.signalAll();
                    return Fiber.resume(null, this);
                } else {
                    return Fiber.sleep(1000, this);
                }
            }
            LogFile logFile = new LogFile(f.fileStartPos, f.fileStartPos + getFileSize(), f.channel,
                    f.file, FiberGroup.currentGroup());
            queue.addLast(logFile);
            if (queue.size() == 1) {
                queueStartPosition = logFile.startPos;
            }
            queueEndPosition = logFile.endPos;
            allocDoneCond.signalAll();
            return Fiber.resume(null, this);
        }
    }

    private class FileAllocFrame extends FiberFrame<Void> {
        private long fileStartPos;

        private File file;
        private AsynchronousFileChannel channel;
        private final int perfType;
        private final long perfStartTime;
        private boolean result;

        public FileAllocFrame() {
            this.perfType = mainLogFile ? PerfConsts.RAFT_D_LOG_FILE_ALLOC : PerfConsts.RAFT_D_IDX_FILE_ALLOC;
            this.perfStartTime = groupConfig.perfCallback.takeTime(perfType);
        }

        @Override
        public FrameCallResult execute(Void v) {
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
                    HashSet<OpenOption> openOptions = new HashSet<>();
                    openOptions.add(StandardOpenOption.READ);
                    openOptions.add(StandardOpenOption.WRITE);
                    openOptions.add(StandardOpenOption.CREATE);
                    ExecutorService executor = groupConfig.ioCallbackUseGroupExecutor ?
                            groupConfig.fiberGroup.getExecutor() : ioExecutor;
                    channel = AsynchronousFileChannel.open(file.toPath(), openOptions, executor);
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
            result = true;
            groupConfig.perfCallback.fireTime(perfType, perfStartTime);
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            log.error("allocate file fail: ", ex);
            if (channel != null) {
                DtUtil.close(channel);
                channel = null;
            }
            return Fiber.frameReturn();
        }
    }

    protected boolean isMarkClose() {
        return markClose;
    }
}
