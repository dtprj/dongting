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
package com.github.dtprj.dongting.raft.sm;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftExecutor;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftStatus;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;
import com.github.dtprj.dongting.raft.store.StatusFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class DefaultSnapshotManager implements SnapshotManager {

    private static final DtLog log = DtLogs.getLogger(DefaultSnapshotManager.class);

    private static final String DATA_SUFFIX = ".data";
    private static final String IDX_SUFFIX = ".idx";

    private static final String KEY_LAST_INDEX = "lastIncludedIndex";
    private static final String KEY_LAST_TERM = "lastIncludedTerm";
    private static final String KEY_MAX_BLOCK = "maxBlock";


    private final RaftGroupConfigEx groupConfig;
    private final ExecutorService ioExecutor;
    private final RaftExecutor raftExecutor;
    private final RaftStatus raftStatus;

    private File snapshotDir;

    private File lastIdxFile;
    private File lastDataFile;
    private SnapshotSaveTask currentSaveTask;

    public DefaultSnapshotManager(RaftGroupConfigEx groupConfig, ExecutorService ioExecutor) {
        this.groupConfig = groupConfig;
        this.ioExecutor = ioExecutor;
        this.raftExecutor = (RaftExecutor) groupConfig.getRaftExecutor();
        this.raftStatus = groupConfig.getRaftStatus();
    }

    @Override
    public FileSnapshot init(Supplier<Boolean> stopIndicator) throws IOException {
        File dataDir = FileUtil.ensureDir(groupConfig.getDataDir());
        snapshotDir = FileUtil.ensureDir(dataDir, "snapshot");
        File[] files = snapshotDir.listFiles(f -> f.isFile() &&
                (f.getName().endsWith(DATA_SUFFIX) || f.getName().endsWith(IDX_SUFFIX)));
        if (files == null || files.length == 0) {
            return null;
        }
        Arrays.sort(files);
        for (int i = files.length - 1; i >= 0; i--) {
            File f = files[i];
            if (!f.getName().endsWith(IDX_SUFFIX)) {
                continue;
            }
            if (f.length() == 0) {
                continue;
            }
            String baseName = FileUtil.baseName(f);
            File dataFile = new File(groupConfig.getDataDir(), baseName + DATA_SUFFIX);
            if (dataFile.exists()) {
                lastIdxFile = f;
                lastDataFile = dataFile;
                break;
            } else {
                log.error("missing data file: {}", f.getPath());
            }
        }
        if (lastIdxFile == null) {
            log.info("no saved snapshot found");
            return null;
        } else {
            log.info("use snapshot: {}", lastDataFile.getPath());
        }

        for (File f : files) {
            if (f != lastIdxFile && f != lastDataFile) {
                deleteInIoExecutor(f);
            }
        }

        try (StatusFile sf = new StatusFile(lastIdxFile, groupConfig.getIoExecutor())) {
            sf.init();
            String lastIndex = sf.getProperties().getProperty(KEY_LAST_INDEX);
            String lastTerm = sf.getProperties().getProperty(KEY_LAST_TERM);
            String maxBlock = sf.getProperties().getProperty(KEY_MAX_BLOCK);
            return new FileSnapshot(Long.parseLong(lastIndex), Integer.parseInt(lastTerm),
                    lastDataFile, ioExecutor, Integer.parseInt(maxBlock), stopIndicator);
        }
    }

    private void deleteInIoExecutor(File f) {
        ioExecutor.submit(() -> {
            if (f != null && f.exists()) {
                log.info("delete file: {}", f.getPath());
                if (!f.delete()) {
                    log.error("delete file failed: {}", f.getPath());
                }
            }
        });
    }

    @Override
    public CompletableFuture<Long> saveSnapshot(StateMachine stateMachine,
                                                Supplier<Boolean> stopIndicator) {
        return new SnapshotSaveTask(stateMachine, stopIndicator).exec();
    }

    private class SnapshotSaveTask {
        private final StateMachine stateMachine;
        private final Supplier<Boolean> stopIndicator;
        private final long startTime = System.currentTimeMillis();
        private final CompletableFuture<Long> future = new CompletableFuture<>();
        private final CRC32C crc32c = new CRC32C();
        private final ByteBuffer headerBuffer = ByteBuffer.allocate(8);

        // access reference in raft thread
        private File newDataFile;
        private File newIdxFile;
        private AsynchronousFileChannel channel;
        private Snapshot currentSnapshot;
        private AsyncIoTask writeTask;


        public SnapshotSaveTask(StateMachine stateMachine, Supplier<Boolean> stopIndicator) {
            this.stateMachine = stateMachine;
            this.stopIndicator = stopIndicator;
        }

        public CompletableFuture<Long> exec() {
            runInRaftThread(this::beginSave);
            return future;
        }

        // run in raft thread
        private void beginSave() {
            // currentSaveTask should access in raft thread
            if (currentSaveTask != null) {
                log.warn("snapshot save task is running");
                future.complete(-1L);
                return;
            }
            currentSaveTask = this;
            try {
                currentSnapshot = stateMachine.takeSnapshot(raftStatus.getCurrentTerm());
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
                String baseName = sdf.format(new Date()) + "_" + currentSnapshot.getId();
                newDataFile = new File(snapshotDir, baseName + DATA_SUFFIX);
                newIdxFile = new File(snapshotDir, baseName + IDX_SUFFIX);

                HashSet<StandardOpenOption> options = new HashSet<>();
                options.add(StandardOpenOption.CREATE_NEW);
                options.add(StandardOpenOption.WRITE);
                channel = AsynchronousFileChannel.open(newDataFile.toPath(), options, ioExecutor);

                writeTask = new AsyncIoTask(channel, stopIndicator, null);

                read(0, 0);
            } catch (Throwable e) {
                log.error("update snapshot failed", e);
                future.completeExceptionally(e);
                reset(false);
            }
        }

        private void read(long currentWritePos, int maxBlock) {
            // readNext() should call in raft thread
            Runnable r = () -> {
                CompletableFuture<RefBuffer> f = currentSnapshot.readNext();
                f.whenCompleteAsync((rb, ex) -> whenReadFinish(rb, ex, currentWritePos, maxBlock, true), ioExecutor);
            };
            runInRaftThread(r);
        }

        private void runInRaftThread(Runnable r) {
            if (raftExecutor.inRaftThread()) {
                r.run();
            } else {
                raftExecutor.execute(r);
            }
        }

        private boolean shouldReturn(Throwable ex) {
            if (stopIndicator.get()) {
                future.cancel(false);
                reset(false);
                return true;
            }
            if (ex != null) {
                future.completeExceptionally(ex);
                reset(false);
                return true;
            }
            return false;
        }

        // run in io thread
        private void whenReadFinish(RefBuffer rb, Throwable ex, long currentWritePos,
                                    int maxBlock, boolean writeHeader) {
            try {
                if (ex != null) {
                    log.error("read snapshot fail", ex);
                }
                if (shouldReturn(ex)) {
                    return;
                }
                if (rb != null && rb.getBuffer() != null && rb.getBuffer().hasRemaining()) {
                    ByteBuffer buffer = rb.getBuffer();
                    if (writeHeader) {
                        crc32c.reset();
                        RaftUtil.updateCrc(crc32c, buffer, buffer.position(), buffer.remaining());
                        headerBuffer.clear();
                        headerBuffer.putInt(buffer.remaining());
                        headerBuffer.putInt((int) crc32c.getValue());
                        CompletableFuture<Void> f = writeTask.write(false, false, headerBuffer, currentWritePos);
                        long nextWritePos = currentWritePos + headerBuffer.remaining();
                        final int newMaxBlock = Math.max(maxBlock, buffer.remaining());
                        f.whenCompleteAsync((v, writeEx) -> whenReadFinish(
                                rb, writeEx, nextWritePos, newMaxBlock, false), ioExecutor);
                    } else {
                        CompletableFuture<Void> f = writeTask.write(false, false, buffer, currentWritePos);
                        long nextWritePos = currentWritePos + buffer.remaining();
                        f.whenCompleteAsync((v, writeEx) -> whenWriteFinish(rb, writeEx, nextWritePos, maxBlock), ioExecutor);
                    }
                } else {
                    afterDataWriteFinish(maxBlock);
                }
            } catch (Throwable unexpect) {
                BugLog.log(unexpect);
                future.completeExceptionally(unexpect);
                reset(false);
            }
        }

        // run in io thread
        private void whenWriteFinish(RefBuffer rb, Throwable ex, long nextWritePos, int maxBlock) {
            try {
                rb.release();
                if (ex != null) {
                    log.error("write snapshot fail", ex);
                }
                if (shouldReturn(ex)) {
                    return;
                }
                read(nextWritePos, maxBlock);
            } catch (Throwable unexpect) {
                BugLog.log(unexpect);
                future.completeExceptionally(unexpect);
                reset(false);
            }
        }

        // run in io thread
        private void afterDataWriteFinish(int maxBlock) {
            try {
                if (shouldReturn(null)) {
                    return;
                }
                channel.force(true);
                log.info("snapshot data file write success: {}", newDataFile.getPath());
                if (shouldReturn(null)) {
                    return;
                }
                try (StatusFile sf = new StatusFile(newIdxFile, groupConfig.getIoExecutor())) {
                    sf.init();
                    sf.getProperties().setProperty(KEY_LAST_INDEX, String.valueOf(currentSnapshot.getLastIncludedIndex()));
                    sf.getProperties().setProperty(KEY_LAST_TERM, String.valueOf(currentSnapshot.getLastIncludedTerm()));
                    sf.getProperties().setProperty(KEY_MAX_BLOCK, String.valueOf(maxBlock));

                    // just for human reading
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
                    sf.getProperties().setProperty("saveStartTime", sdf.format(new Date(startTime)));
                    sf.getProperties().setProperty("saveEndTime", sdf.format(new Date()));

                    // TODO make async?
                    sf.update(sf.getProperties(), true).get(10, TimeUnit.SECONDS);
                    log.info("snapshot status file write success: {}", newIdxFile.getPath());
                }

                future.complete(currentSnapshot.getLastIncludedIndex());
                reset(true);

                File oldIdxFile = lastIdxFile;
                File oldDataFile = lastDataFile;
                lastIdxFile = newIdxFile;
                lastDataFile = newDataFile;
                deleteInIoExecutor(oldIdxFile);
                deleteInIoExecutor(oldDataFile);
            } catch (Throwable ex) {
                log.error("finish save snapshot fail", ex);
                future.completeExceptionally(ex);
                reset(false);
            }
        }

        private void reset(boolean success) {
            runInRaftThread(() -> {
                currentSaveTask = null;
                DtUtil.close(channel, currentSnapshot);
                if (!success) {
                    deleteInIoExecutor(newDataFile);
                    deleteInIoExecutor(newIdxFile);
                }
            });
        }
    }

}
