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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
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
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
    protected final Supplier<Boolean> stopIndicator;
    protected final RaftGroupConfig groupConfig;
    private final RaftStatusImpl raftStatus;

    protected final long fileSize;
    protected final long fileLenMask;
    protected final int fileLenShiftBits;

    protected long queueStartPosition;
    protected long queueEndPosition;

    private FiberFuture<LogFile> allocateFuture;
    private boolean allocating;
    private boolean deleting;
    private final FiberCondition fileOpsCondition;

    public FileQueue(File dir, RaftGroupConfig groupConfig, long fileSize) {
        this.dir = dir;
        this.ioExecutor = groupConfig.getIoExecutor();
        this.stopIndicator = groupConfig.getStopIndicator();
        this.groupConfig = groupConfig;
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();

        this.fileSize = fileSize;
        this.fileLenMask = fileSize - 1;
        this.fileLenShiftBits = BitUtil.zeroCountOfBinary(fileSize);
        this.fileOpsCondition = groupConfig.getFiberGroup().newCondition();
    }

    protected final long getFileSize() {
        return fileSize;
    }

    protected final long startPosOfFile(long pos) {
        return pos & (~fileLenMask);
    }


    public void init(String name) throws IOException {
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
                queue.addLast(new LogFile(startPos, startPos + getFileSize(), channel, f));
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

    private class FileOpsFiberFrame extends FiberFrame<Void> {
        @Override
        public FrameCallResult execute(Void input) {
            return null;
        }
    }

    private void finishAllocateIfDone() {
        if (allocateFuture != null && allocateFuture.isDone()) {
            if (allocateFuture.getEx() == null) {
                LogFile newFile = allocateFuture.getResult();
                queue.addLast(newFile);
                queueEndPosition = newFile.endPos;
            }
            allocateFuture = null;
            allocating = false;
            fileOpsCondition.signalAll();
            // exception is logged in allocate() method
        }
    }

    protected boolean checkPosReady(long pos) {
        if (allocateFuture == null) {
            if (pos >= queueEndPosition - fileSize && !deleting) {
                // maybe pre allocate next file
                allocate();
            }
        }
        finishAllocateIfDone();
        return pos < queueEndPosition;
    }

    protected FiberFrame<Void> ensureWritePosReady(long pos, boolean retry) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                finishAllocateIfDone();
                if (pos >= queueEndPosition) {
                    if (allocating || deleting) {
                        // resume on this method
                        return fileOpsCondition.awaitOn(this);
                    } else {
                        return Fiber.call(allocateSync(retry), this::resume);
                    }
                } else {
                    return Fiber.frameReturn();
                }
            }

            private FrameCallResult resume(LogFile logFile) {
                finishAllocateIfDone();
                // loop
                return execute(null);
            }
        };
    }

    private FiberFrame<LogFile> allocateSync(boolean retry) {
        long[] retryInterval = retry ? groupConfig.getIoRetryInterval() : null;
        return new IoRetryFrame<>(retryInterval, groupConfig.getAllocateTimeout(),
                groupConfig.getStopCondition(), () -> {
            allocate();
            return allocateFuture;
        });
    }

    private void allocate() {
        allocating = true;
        allocateFuture = groupConfig.getFiberGroup().newFuture();
        try {
            ioExecutor.execute(() -> doAllocateInIoThread(queueEndPosition, allocateFuture));
        } catch (Throwable e) {
            log.error("submit allocate task fail: ", e);
            allocateFuture.completeExceptionally(e);
        }
    }

    // this method run in io executor
    private void doAllocateInIoThread(long fileStartPos, FiberFuture<LogFile> future) {
        AsynchronousFileChannel channel = null;
        // in io thread we can't use ts of raft dispatcher thread
        long startTime = System.currentTimeMillis();
        try {
            File f = new File(dir, String.format("%020d", fileStartPos));
            HashSet<OpenOption> openOptions = new HashSet<>();
            openOptions.add(StandardOpenOption.READ);
            openOptions.add(StandardOpenOption.WRITE);
            openOptions.add(StandardOpenOption.CREATE);
            channel = AsynchronousFileChannel.open(f.toPath(), openOptions, ioExecutor);
            LogFile logFile = new LogFile(fileStartPos, fileStartPos + getFileSize(), channel, f);

            ByteBuffer buf = ByteBuffer.allocate(1);

            AsyncIoTask t = new AsyncIoTask(logFile.channel, null);
            t.writeAndFlush(buf, getFileSize() - 1, true, new CompletionHandler<>() {
                @Override
                public void completed(Void result, Void attachment) {
                    long time = System.currentTimeMillis() - startTime;
                    log.info("allocate log file done, cost {} ms: {}", time, logFile.file.getPath());
                    future.complete(logFile);
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    long time = System.currentTimeMillis() - startTime;
                    log.info("allocate log file failed, cost {} ms: {}", time, logFile.file.getPath(), exc);
                    future.completeExceptionally(exc);
                }
            });
        } catch (Throwable e) {
            log.error("allocate log file fail: ", e);
            if (channel != null) {
                DtUtil.close(channel);
            }
            future.completeExceptionally(e);
        }
    }

    @Override
    public void close() {
        for (int i = 0; i < queue.size(); i++) {
            DtUtil.close(queue.get(i).channel);
        }
    }

    protected void submitDeleteTask(Predicate<LogFile> shouldDelete) {
        if (deleting) {
            log.info("deleting, ignore delete request");
            return;
        }
        if (stopIndicator.get()) {
            return;
        }
        if (queue.size() <= 1) {
            return;
        }
        LogFile logFile = queue.get(0);
        if (!shouldDelete.test(logFile)) {
            return;
        }
        deleting = true;
        long stateMachineEpoch = raftStatus.getStateMachineEpoch();
        FiberFuture<Void> deleteFuture = groupConfig.getFiberGroup().newFuture();
        deleteFuture.registerCallback(() -> processDeleteResult(deleteFuture.getEx() == null,
                shouldDelete, stateMachineEpoch));
        try {
            ioExecutor.execute(() -> {
                try {
                    delete(logFile);
                    deleteFuture.complete(null);
                } catch (Throwable e) {
                    log.error("delete file fail: ", logFile.file.getPath(), e);
                    deleteFuture.completeExceptionally(e);
                }
            });
        } catch (Throwable e) {
            log.error("submit delete task fail: ", e);
            deleteFuture.completeExceptionally(e);
        }
    }

    protected void delete(LogFile logFile) throws IOException {
        log.debug("close log file: {}", logFile.file.getPath());
        DtUtil.close(logFile.channel);
        log.info("delete log file: {}", logFile.file.getPath());
        Files.delete(logFile.file.toPath());
    }

    private void processDeleteResult(boolean success, Predicate<LogFile> shouldDelete, long stateMachineEpoch) {
        // access variable deleting in raft thread
        deleting = false;
        fileOpsCondition.signalAll();
        if (stateMachineEpoch != raftStatus.getStateMachineEpoch()) {
            log.info("state machine epoch changed, ignore process delete result");
            return;
        }
        if (success) {
            queue.removeFirst();
            queueStartPosition = queue.get(0).startPos;
            submitDeleteTask(shouldDelete);
            afterDelete();
        }
    }

    protected void afterDelete() {
    }

    protected FiberFrame<Void> forceDeleteAll() throws Exception {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                if(deleting || allocating) {
                    return fileOpsCondition.awaitOn(this);
                } else {
                    // sync operation so don't need to set deleting flag
                    for (int i = 0; i < queue.size(); i++) {
                        LogFile lf = queue.get(0);
                        if (lf.use > 0) {
                            log.warn("file is in use: {}", lf.file.getName());
                        }
                        delete(lf);
                    }
                    return Fiber.frameReturn();
                }
            }
        };
    }
}
