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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
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
    protected final IndexedQueue<LogFile> queue = new IndexedQueue<>();
    protected final File dir;

    protected final ExecutorService ioExecutor;
    protected final Executor raftExecutor;
    protected final Supplier<Boolean> stopIndicator;

    protected long queueStartPosition;
    protected long queueEndPosition;

    protected CompletableFuture<LogFile> allocateFuture;

    private boolean deleting;

    public FileQueue(File dir, ExecutorService ioExecutor, RaftGroupConfigEx groupConfig) {
        this.dir = dir;
        this.ioExecutor = ioExecutor;
        this.raftExecutor = groupConfig.getRaftExecutor();
        this.stopIndicator = groupConfig.getStopIndicator();
    }

    protected abstract long getFileSize();

    protected abstract long getWritePos();

    protected abstract int getFileLenShiftBits();

    public void init() throws IOException {
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return;
        }
        Arrays.sort(files);
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
                log.info("load file: {}", f.getPath());
                HashSet<OpenOption> openOptions = new HashSet<>();
                openOptions.add(StandardOpenOption.READ);
                openOptions.add(StandardOpenOption.WRITE);
                AsynchronousFileChannel channel = AsynchronousFileChannel.open(f.toPath(), openOptions, ioExecutor);
                queue.addLast(new LogFile(startPos, startPos + getFileSize(), channel, f));
            }
        }
        for (int i = 0; i < queue.size(); i++) {
            LogFile lf = queue.get(i);
            if ((lf.startPos & (getFileSize() - 1)) != 0) {
                throw new RaftException("file start index error: " + lf.startPos);
            }
            if (i != 0 && lf.startPos != queue.get(i - 1).endPos) {
                throw new RaftException("not follow previous file " + lf.startPos);
            }
        }

        if (queue.size() > 0) {
            queueStartPosition = queue.get(0).startPos;
            queueEndPosition = queue.get(queue.size() - 1).endPos;
        }
    }

    protected LogFile getLogFile(long filePos) {
        int index = (int) ((filePos - queueStartPosition) >>> getFileLenShiftBits());
        return queue.get(index);
    }

    protected void ensureWritePosReady(long pos) throws InterruptedException, IOException {
        try {
            while (pos >= queueEndPosition) {
                if (allocateFuture == null) {
                    allocateFuture = allocate(queueEndPosition);
                }
                processAllocResult();
            }
            // pre allocate next file
            if (allocateFuture == null) {
                allocateFuture = allocate(queueEndPosition);
            }
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    private void processAllocResult() throws InterruptedException, ExecutionException {
        try {
            LogFile newFile = allocateFuture.get();
            queue.addLast(newFile);
            queueEndPosition = newFile.endPos;
        } finally {
            allocateFuture = null;
        }
    }

    private CompletableFuture<LogFile> allocate(long currentEndPosition) {
        CompletableFuture<LogFile> future = new CompletableFuture<>();
        ioExecutor.execute(() -> {
            AsynchronousFileChannel channel = null;
            try {
                File f = new File(dir, String.format("%020d", currentEndPosition));
                HashSet<OpenOption> openOptions = new HashSet<>();
                openOptions.add(StandardOpenOption.READ);
                openOptions.add(StandardOpenOption.WRITE);
                openOptions.add(StandardOpenOption.CREATE);
                channel = AsynchronousFileChannel.open(f.toPath(), openOptions, ioExecutor);
                ByteBuffer buf = ByteBuffer.allocate(1);
                LogFile logFile = new LogFile(currentEndPosition, currentEndPosition + getFileSize(), channel, f);
                AsyncIoTask t = new AsyncIoTask(logFile.channel, stopIndicator);
                t.write(true, true, buf, getFileSize() - 1).whenComplete((v, ex) -> {
                    if (ex != null) {
                        future.completeExceptionally(ex);
                    } else {
                        future.complete(logFile);
                    }
                });
            } catch (Throwable e) {
                if (channel != null) {
                    DtUtil.close(channel);
                }
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @Override
    public void close() {
        for (int i = 0; i < queue.size(); i++) {
            DtUtil.close(queue.get(i).channel);
        }
    }

    protected void submitDeleteTask(Predicate<LogFile> shouldDelete) {
        if (deleting) {
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
        ioExecutor.execute(() -> {
            try {
                if (stopIndicator.get()) {
                    return;
                }
                log.debug("close log file: {}", logFile.file.getPath());
                DtUtil.close(logFile.channel);
                log.info("delete log file: {}", logFile.file.getPath());
                boolean b = logFile.file.delete();
                if (!b) {
                    log.warn("delete log file failed: {}", logFile.file.getPath());
                }
                raftExecutor.execute(() -> processDeleteResult(b, shouldDelete));
            } catch (Throwable e) {
                BugLog.log(e);
                raftExecutor.execute(() -> processDeleteResult(false, shouldDelete));
            }
        });
    }

    private void processDeleteResult(boolean success, Predicate<LogFile> shouldDelete) {
        // access variable deleting in raft thread
        deleting = false;
        if (success) {
            queue.removeFirst();
            queueStartPosition = queue.get(0).startPos;
            submitDeleteTask(shouldDelete);
        }
    }

}
