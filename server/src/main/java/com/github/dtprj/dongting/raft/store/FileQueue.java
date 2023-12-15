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
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.DoInLockFrame;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FiberLock;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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
abstract class FileQueue implements AutoCloseable {
    private static final DtLog log = DtLogs.getLogger(FileQueue.class);
    private static final Pattern PATTERN = Pattern.compile("^(\\d{20})$");
    protected final IndexedQueue<LogFile> queue = new IndexedQueue<>(32);
    protected final File dir;

    protected final ExecutorService ioExecutor;
    protected final RaftGroupConfig groupConfig;
    private final RaftStatusImpl raftStatus;

    protected final long fileSize;
    protected final long fileLenMask;
    protected final int fileLenShiftBits;

    protected long queueStartPosition;
    protected long queueEndPosition;

    private FiberFuture<Void> allocateFuture;
    private FiberLock fileOpsLock;

    protected boolean initialized;

    public FileQueue(File dir, RaftGroupConfig groupConfig, long fileSize) {
        this.dir = dir;
        this.ioExecutor = groupConfig.getIoExecutor();
        this.groupConfig = groupConfig;
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();

        this.fileSize = fileSize;
        this.fileLenMask = fileSize - 1;
        this.fileLenShiftBits = BitUtil.zeroCountOfBinary(fileSize);

        FiberGroup g = groupConfig.getFiberGroup();
        this.fileOpsLock = g.newLock();
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
                AsynchronousFileChannel channel = AsynchronousFileChannel.open(f.toPath(), openOptions, ioExecutor);
                queue.addLast(new LogFile(startPos, startPos + getFileSize(), channel,
                        f, groupConfig.getFiberGroup().newCondition()));
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
                    queue.get(0).file.getName(), queue.get(queue.size() - 1).file.getName());
        }
    }

    protected LogFile getLogFile(long filePos) {
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
                        boolean retry = initialized && !isGroupShouldStopPlain();
                        return Fiber.call(allocateSync(retry), this);
                    }
                } else {
                    return Fiber.frameReturn();
                }
            }
        };
    }

    private FiberFrame<Void> allocateSync(boolean retry) {
        long[] retryInterval = retry ? groupConfig.getIoRetryInterval() : null;
        allocateFuture = groupConfig.getFiberGroup().newFuture();
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
        allocateFuture = groupConfig.getFiberGroup().newFuture();
        Fiber f = new Fiber("allocate-file", groupConfig.getFiberGroup(), new FiberFrame<Void>() {
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
        private final FiberGroup fiberGroup = getFiberGroup();

        private long startTime;
        private long fileStartPos;
        private String fileName;

        private File file;
        private AsynchronousFileChannel channel;

        private LogFile logFile;

        public AllocateFrame() {
            super(fileOpsLock);
        }

        @Override
        protected FrameCallResult afterGetLock() {
            startTime = System.currentTimeMillis();
            fileStartPos = queueEndPosition;
            fileName = String.format("%020d", fileStartPos);

            FiberFuture<Pair<File, AsynchronousFileChannel>> createFileFuture = fiberGroup.newFuture();
            ioExecutor.execute(() -> {
                try {
                    File f = new File(dir, fileName);
                    HashSet<OpenOption> openOptions = new HashSet<>();
                    openOptions.add(StandardOpenOption.READ);
                    openOptions.add(StandardOpenOption.WRITE);
                    openOptions.add(StandardOpenOption.CREATE);
                    AsynchronousFileChannel channel = AsynchronousFileChannel.open(f.toPath(), openOptions, ioExecutor);
                    createFileFuture.fireComplete(new Pair<>(f, channel));
                } catch (Throwable e) {
                    createFileFuture.fireCompleteExceptionally(e);
                }
            });
            return createFileFuture.await(this::afterCreateFile);
        }

        private FrameCallResult afterCreateFile(Pair<File, AsynchronousFileChannel> input) {
            file = input.getLeft();
            channel = input.getRight();
            logFile = new LogFile(fileStartPos, fileStartPos + getFileSize(), channel,
                    file, fiberGroup.newCondition());
            ByteBuffer buf = ByteBuffer.allocate(1);
            // no retry here, allocateSync() will retry
            AsyncIoTask t = new AsyncIoTask(groupConfig.getFiberGroup(), logFile.channel);
            FiberFuture<Void> writeFuture = t.writeAndFlush(buf, getFileSize() - 1, true);
            return writeFuture.await(this::afterWrite);
        }

        private FrameCallResult afterWrite(Void unused) {
            long time = System.currentTimeMillis() - startTime;
            log.info("allocate file done, cost {} ms: {}", time, logFile.file.getPath());
            queue.addLast(logFile);
            queueEndPosition = logFile.endPos;
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult handle(Throwable ex) throws Throwable {
            long time = System.currentTimeMillis() - startTime;
            if (channel != null) {
                DtUtil.close(channel);
            }
            log.info("allocate file failed, cost {} ms: {}", time, logFile.file.getPath(), ex);
            throw ex;
        }
    }

    @Override
    public void close() {
        for (int i = 0; i < queue.size(); i++) {
            DtUtil.close(queue.get(i).channel);
        }
    }

    protected FiberFrame<Void> delete(LogFile logFile) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void v) throws Throwable {
                return logFile.awaitNotUse(this::afterNotUse);
            }

            private FrameCallResult afterNotUse(Void unused) {
                FiberFuture<Void> deleteFuture = groupConfig.getFiberGroup().newFuture();
                FiberGroup fiberGroup = getFiberGroup();
                // mark deleted first, so that other fibers will not use this file
                logFile.deleted = true;
                try {
                    ioExecutor.execute(() -> {
                        try {
                            log.debug("close log file: {}", logFile.file.getPath());
                            DtUtil.close(logFile.channel);
                            log.info("delete log file: {}", logFile.file.getPath());
                            Files.delete(logFile.file.toPath());

                            deleteFuture.fireComplete(null);
                        } catch (Throwable e) {
                            log.error("delete file fail: ", logFile.file.getPath(), e);
                            deleteFuture.fireCompleteExceptionally(e);
                        }
                    });
                } catch (Throwable e) {
                    log.error("submit delete task fail: ", e);
                    deleteFuture.completeExceptionally(e);
                }
                return deleteFuture.await(this::afterDelete);
            }

            private FrameCallResult afterDelete(Void unused) {
                if (queue.size() > 1) {
                    queue.removeFirst();
                }
                if (queue.size() > 1) {
                    queueStartPosition = queue.get(0).startPos;
                } else {
                    queueStartPosition = 0;
                    queueEndPosition = 0;
                }
                return Fiber.frameReturn();
            }
        };
    }

    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }
}
