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
import com.github.dtprj.dongting.fiber.DoInLockFrame;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FiberLock;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.PerfConsts;
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
import java.util.function.Predicate;
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

    protected long queueStartPosition;
    protected long queueEndPosition;

    private FiberFuture<Void> allocateFuture;
    protected final FiberLock allocateLock;

    protected boolean initialized;

    public FileQueue(File dir, RaftGroupConfigEx groupConfig, long fileSize, boolean mainLogFile) {
        this.dir = dir;
        this.ioExecutor = groupConfig.getIoExecutor();
        this.groupConfig = groupConfig;
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();

        this.fileSize = fileSize;
        this.fileLenMask = fileSize - 1;
        this.fileLenShiftBits = BitUtil.zeroCountOfBinary(fileSize);
        this.mainLogFile = mainLogFile;

        FiberGroup g = groupConfig.getFiberGroup();
        this.allocateLock = g.newLock("allocFile");
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
                ExecutorService executor = groupConfig.isIoCallbackUseGroupExecutor() ?
                        groupConfig.getFiberGroup().getExecutor() : ioExecutor;
                AsynchronousFileChannel channel = AsynchronousFileChannel.open(f.toPath(), openOptions, executor);
                queue.addLast(new LogFile(startPos, startPos + getFileSize(), channel,
                        f, groupConfig.getFiberGroup()));
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

    protected LogFile getLogFile(long filePos) {
        if (filePos < queueStartPosition || filePos >= queueEndPosition) {
            return null;
        }
        int index = (int) ((filePos - queueStartPosition) >>> fileLenShiftBits);
        return queue.get(index);
    }

    protected void tryAllocateAsync(long pos) {
        if (allocateFuture == null) {
            if (pos >= queueEndPosition - fileSize) {
                // maybe pre allocate next file
                allocateAsync();
            }
        }
    }

    protected FiberFrame<Void> ensureWritePosReady(long pos) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (pos >= queueEndPosition) {
                    if (allocateFuture != null) {
                        // resume on this method
                        return allocateFuture.await(this);
                    } else {
                        int perfType = mainLogFile ? PerfConsts.RAFT_C_LOG_POS_NOT_READY : PerfConsts.RAFT_C_IDX_POS_NOT_READY;
                        groupConfig.getPerfCallback().fireCount(perfType);
                        boolean retry = initialized && !isGroupShouldStopPlain();
                        return Fiber.call(allocateSync(retry), this);
                    }
                } else {
                    tryAllocateAsync(pos);
                    return Fiber.frameReturn();
                }
            }
        };
    }

    private FiberFrame<Void> allocateSync(boolean retry) {
        long[] retryInterval = retry ? groupConfig.getIoRetryInterval() : null;
        allocateFuture = groupConfig.getFiberGroup().newFuture("allocFileSync");
        return new RetryFrame<>(new AllocateFrame(), retryInterval, true) {
            @Override
            protected FrameCallResult doFinally() {
                allocateFuture.complete(null);
                allocateFuture = null;
                return super.doFinally();
            }
        };
    }

    private void allocateAsync() {
        allocateFuture = groupConfig.getFiberGroup().newFuture("allocFileAsync");
        Fiber f = new Fiber("allocateFile", groupConfig.getFiberGroup(), new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(new AllocateFrame(), this::justReturn);
            }

            @Override
            protected FrameCallResult doFinally() {
                allocateFuture.complete(null);
                allocateFuture = null;
                return Fiber.frameReturn();
            }
        });
        f.start();
    }

    private class AllocateFrame extends DoInLockFrame<Void> {
        private long fileStartPos;

        private File file;
        private AsynchronousFileChannel channel;
        private final int perfType;
        private final long perfStartTime;

        public AllocateFrame() {
            super(allocateLock);
            this.perfType = mainLogFile ? PerfConsts.RAFT_D_LOG_FILE_ALLOC : PerfConsts.RAFT_D_IDX_FILE_ALLOC;
            this.perfStartTime = groupConfig.getPerfCallback().takeTime(perfType);
        }

        @Override
        protected FrameCallResult afterGetLock() {
            if (raftStatus.isInstallSnapshot()) {
                return Fiber.frameReturn();
            }
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
                    ExecutorService executor = groupConfig.isIoCallbackUseGroupExecutor() ?
                            groupConfig.getFiberGroup().getExecutor() : ioExecutor;
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

        private FrameCallResult afterCreateFile(Void v) {
            groupConfig.getPerfCallback().fireDuration(perfType, perfStartTime);
            LogFile logFile = new LogFile(fileStartPos, fileStartPos + getFileSize(), channel,
                    file, getFiberGroup());
            queue.addLast(logFile);
            queueEndPosition = logFile.endPos;
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult handle(Throwable ex) throws Throwable {
            if (channel != null) {
                DtUtil.close(channel);
            }
            throw ex;
        }
    }

    protected void closeChannel() {
        for (int i = 0; i < queue.size(); i++) {
            DtUtil.close(queue.get(i).getChannel());
        }
    }

    private class DeleteFrame extends FiberFrame<Void> {

        private final LogFile logFile;

        public DeleteFrame(LogFile logFile) {
            this.logFile = logFile;
        }

        @Override
        public FrameCallResult execute(Void input) {
            FiberFuture<Void> deleteFuture = groupConfig.getFiberGroup().newFuture("deleteFile");
            try {
                ioExecutor.execute(() -> {
                    try {
                        log.debug("close log file: {}", logFile.getFile().getPath());
                        DtUtil.close(logFile.getChannel());
                        log.info("delete log file: {}", logFile.getFile().getPath());
                        Files.delete(logFile.getFile().toPath());

                        deleteFuture.fireComplete(null);
                    } catch (Throwable e) {
                        log.error("delete file fail: ", logFile.getFile().getPath(), e);
                        deleteFuture.fireCompleteExceptionally(e);
                    }
                });
            } catch (Throwable e) {
                log.error("submit delete task fail: ", e);
                deleteFuture.completeExceptionally(e);
            }
            return deleteFuture.await(this::doAfterDelete);
        }

        private FrameCallResult doAfterDelete(Void unused) {
            if (queue.size() > 1) {
                queue.removeFirst();
            }
            if (queue.size() > 1) {
                queueStartPosition = queue.get(0).startPos;
            } else {
                queueStartPosition = 0;
                queueEndPosition = 0;
            }
            FileQueue.this.afterDelete();
            return Fiber.frameReturn();
        }
    }

    protected FiberFrame<Void> delete(LogFile logFile) {
        return new DoInLockFrame<>(logFile.getLock()) {
            @Override
            protected FrameCallResult afterGetLock() {
                // mark deleted first, so that other fibers will not use this file
                logFile.deleted = true;
                return Fiber.call(new DeleteFrame(logFile), this::justReturn);
            }
        };
    }

    protected void afterDelete() {
    }

    public FiberFrame<Void> deleteByPredicate(Predicate<LogFile> shouldDelete) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (queue.size() > 0) {
                    LogFile first = queue.get(0);
                    if (shouldDelete.test(first)) {
                        return Fiber.call(delete(first), this);
                    } else {
                        return Fiber.frameReturn();
                    }
                } else {
                    return Fiber.frameReturn();
                }
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                log.error("delete log file fail: ", ex);
                return Fiber.frameReturn();
            }
        };
    }

    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }

    public FiberFrame<Void> beginInstall() {
        return new DoInLockFrame<>(allocateLock) {
            @Override
            protected FrameCallResult afterGetLock() {
                return Fiber.call(deleteByPredicate(logFile -> true), this::justReturn);
            }
        };
    }

}
