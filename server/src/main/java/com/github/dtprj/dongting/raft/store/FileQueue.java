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
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.FutureFrame;
import com.github.dtprj.dongting.fiber.PostFiberFrame;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
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
    protected final boolean mainLogFile;

    protected long queueStartPosition;
    protected long queueEndPosition;

    protected boolean initialized;

    protected boolean markClose;

    private LogFile lruHead;
    private LogFile lruTail;
    private int openFileCount;
    private static final long IDLE_CLOSE_MILLIS = 60_000;

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
    }

    protected final long getFileSize() {
        return fileSize;
    }

    protected final long startPosOfFile(long pos) {
        return pos & (~fileLenMask);
    }

    protected void initQueue() {
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
                long startPos = Long.parseLong(matcher.group(1));
                LogFile lf = new LogFile(startPos, startPos + getFileSize(), f,
                        groupConfig.fiberGroup, ioExecutor, this::lruTouch,
                        raftStatus.ts.wallClockMillis, mainLogFile);
                queue.addLast(lf);
                count++;
            }
        }
        for (int i = 0; i < queue.size(); i++) {
            LogFile lf = queue.get(i);
            long len = lf.getFile().length();
            if (len != getFileSize()) {
                // a crash during pre-allocation may leave the last file with length 0
                // (file created but setLength not durable), no data has been written to
                // it yet, so re-extend it to the full size. any other wrong size is
                // not possible on the normal path and indicates real corruption.
                if (i != queue.size() - 1 || len != 0) {
                    throw new RaftException("file size error: " + lf.getFile().getPath()
                            + ", size=" + len);
                }
                rebuildLastFile(lf.getFile());
            }
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

    private void rebuildLastFile(File f) {
        log.warn("last file size is {}, expect {}, re-extend it: {}",
                f.length(), getFileSize(), f.getPath());
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.setLength(getFileSize());
            raf.getFD().sync();
        } catch (IOException e) {
            throw new RaftException("re-extend file fail: " + f.getPath(), e);
        }
    }

    protected FiberFuture<Void> stopFileQueue() {
        return FutureFrame.startWaitFiber("waitNoRwAndClose-" + groupConfig.groupId,
                groupConfig.fiberGroup, new WaitNoRwAndCloseFrame());
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
                    return Fiber.call(new DeleteFrame(f, ioExecutor), this);
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

    void lruAddLast(LogFile lf) {
        if (lf.lruPrev != null || lf.lruNext != null || lruHead == lf) {
            return; // already in list
        }
        if (lruTail == null) {
            lruHead = lruTail = lf;
        } else {
            lruTail.lruNext = lf;
            lf.lruPrev = lruTail;
            lruTail = lf;
        }
        openFileCount++;
    }

    private void lruRemove(LogFile lf) {
        if (lf.lruPrev == null && lf.lruNext == null && lruHead != lf) {
            return; // not in list
        }
        if (lf.lruPrev != null) {
            lf.lruPrev.lruNext = lf.lruNext;
        } else {
            lruHead = lf.lruNext;
        }
        if (lf.lruNext != null) {
            lf.lruNext.lruPrev = lf.lruPrev;
        } else {
            lruTail = lf.lruPrev;
        }
        lf.lruPrev = null;
        lf.lruNext = null;
        openFileCount--;
    }

    private void lruMoveToLast(LogFile lf) {
        if (lf == lruTail) {
            return;
        }
        lruRemove(lf);
        lruAddLast(lf);
    }

    void lruTouch(LogFile lf) {
        lf.lastAccessTime = raftStatus.ts.wallClockMillis;
        if (lf.lruPrev == null && lf.lruNext == null && lruHead != lf) {
            lruAddLast(lf);
        } else {
            lruMoveToLast(lf);
        }
    }

    void closeIdleFiles() {
        long now = raftStatus.ts.wallClockMillis;
        int size = queue.size();
        if (size == 0 || openFileCount == 0) {
            return;
        }
        if (groupConfig.keepOpenFiles >= size) {
            return;
        }
        // the last 'keepOpenFiles' files are always kept open
        long protectedStartPos;
        if (groupConfig.keepOpenFiles <= 0) {
            protectedStartPos = Long.MAX_VALUE;
        } else {
            protectedStartPos = queue.get(size - groupConfig.keepOpenFiles).startPos;
        }

        int maxIterations = openFileCount;
        while (lruHead != null && maxIterations-- > 0) {
            LogFile lf = lruHead;
            if (lf.startPos >= protectedStartPos) {
                // protected file at LRU head: move to tail and continue
                lruMoveToLast(lf);
                lf.lastAccessTime = raftStatus.ts.wallClockMillis;
                continue;
            }
            if (now - lf.lastAccessTime < IDLE_CLOSE_MILLIS) {
                break;
            }
            if (lf.inUse()) {
                lruMoveToLast(lf);
                lf.lastAccessTime = raftStatus.ts.wallClockMillis;
                continue;
            }
            lruRemove(lf);
            lf.close();
            log.info("close idle file: {}", lf.getFile().getPath());
        }
    }

    // shutdown path: submission of new I/O has stopped, no concurrent access
    private void closeAllChannel() {
        for (int i = 0; i < queue.size(); i++) {
            LogFile lf = queue.get(i);
            lf.destroy();
            lf.lruPrev = null;
            lf.lruNext = null;
        }
        lruHead = null;
        lruTail = null;
        openFileCount = 0;
    }

    /**
     * Waits until all LogFiles in the queue have no active readers or writers,
     * then closes all channels. Used during shutdown and install snapshot.
     */
    private class WaitNoRwAndCloseFrame extends FiberFrame<Void> {

        @Override
        public FrameCallResult execute(Void input) {
            for (int i = 0; i < queue.size(); i++) {
                LogFile lf = queue.get(i);
                if (lf.inUse()) {
                    log.info("file in use during close, wait. reader={}, writer={}, file={}",
                            lf.getReaders(), lf.getWriters(), lf.getFile().getPath());
                    return lf.getNoRwCond().await(this);
                }
            }
            closeAllChannel();
            return Fiber.frameReturn();
        }
    }

    public static final class DeleteFrame extends FiberFrame<Void> {

        private final File file;
        private final boolean recursive;
        private final ExecutorService ioExecutor;
        private final boolean keepRootWhenRecursive;

        public DeleteFrame(File file, ExecutorService ioExecutor) {
            this(file, ioExecutor, false, false);
        }

        public DeleteFrame(File file, ExecutorService ioExecutor, boolean recursive, boolean keepRootWhenRecursive) {
            this.file = file;
            this.recursive = recursive;
            this.ioExecutor = ioExecutor;
            this.keepRootWhenRecursive = keepRootWhenRecursive;
        }

        @Override
        public FrameCallResult execute(Void input) {
            FiberFuture<Void> deleteFuture = FiberGroup.currentGroup().newFuture("deleteFile");
            try {
                ioExecutor.execute(() -> {
                    try {
                        if (recursive) {
                            deleteRecursively(file, true);
                        } else {
                            deleteFile(file);
                        }
                        deleteFuture.fireComplete(null);
                    } catch (Throwable e) {
                        log.error("delete file fail: {}", file.getPath(), e);
                        deleteFuture.fireCompleteExceptionally(e);
                    }
                });
            } catch (Throwable e) {
                log.error("submit delete task fail: ", e);
                deleteFuture.completeExceptionally(e);
            }
            return deleteFuture.await(this::justReturn);
        }

        private void deleteRecursively(File f, boolean root) throws IOException {
            if (f.isDirectory()) {
                File[] children = f.listFiles();
                if (children != null) {
                    for (File child : children) {
                        deleteRecursively(child, false);
                    }
                }
            }
            if (!root || !keepRootWhenRecursive) {
                deleteFile(f);
            }
        }

        private void deleteFile(File f) throws IOException {
            try {
                log.info("delete file: {}", f.getPath());
                Files.delete(f.toPath());
            } catch (NoSuchFileException e) {
                log.warn("file not exists: {}", f.getPath());
            }
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
                lruRemove(first);
                first.destroy();
                return Fiber.call(new DeleteFrame(first.getFile(), ioExecutor), this::justReturn);
            }
        };
        f = new RetryFrame<>(f, groupConfig.ioRetryInterval, true,
                () -> !initialized || raftStatus.installSnapshot);
        f = new PostFiberFrame<>(f) {
            @Override
            protected FrameCallResult postProcess(Void v) {
                queue.pollFirst();
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

    protected boolean isMarkClose() {
        return markClose;
    }
}
