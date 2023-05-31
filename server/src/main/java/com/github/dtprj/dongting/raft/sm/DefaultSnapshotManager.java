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
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.StatusFile;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;

import java.io.File;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class DefaultSnapshotManager implements SnapshotManager {

    private static final DtLog log = DtLogs.getLogger(DefaultSnapshotManager.class);

    private static final String DATA_SUFFIX = ".data";
    private static final String IDX_SUFFIX = ".idx";

    private static final String KEY_LAST_INDEX = "lastIncludedIndex";
    private static final String KEY_LAST_TERM = "lastIncludedTerm";
    private static final String KEY_SAVE_START_TIME = "saveStartTime";
    private static final String KEY_SAVE_END_TIME = "saveEndTime";

    private final RaftGroupConfigEx groupConfig;
    private final ExecutorService ioExecutor;

    private File snapshotDir;
    private File lastIdxFile = null;
    private File lastDataFile = null;

    private SnapshotSaveTask currentSaveTask;

    public DefaultSnapshotManager(RaftGroupConfigEx groupConfig, ExecutorService ioExecutor) {
        this.groupConfig = groupConfig;
        this.ioExecutor = ioExecutor;
    }

    @Override
    public DefaultSnapshot init(Supplier<Boolean> cancelIndicator) throws IOException {
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
            if (f.getName().endsWith(IDX_SUFFIX)) {
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
        }

        for (File f : files) {
            if (f != lastIdxFile && f != lastDataFile) {
                deleteInIoExecutor(f);
            }
        }

        try (StatusFile sf = new StatusFile(lastIdxFile)) {
            sf.init();
            String lastIndex = sf.getProperties().getProperty(KEY_LAST_INDEX);
            String lastTerm = sf.getProperties().getProperty(KEY_LAST_TERM);
            return new DefaultSnapshot(Long.parseLong(lastIndex), Integer.parseInt(lastTerm),
                    lastIdxFile, lastDataFile);
        }
    }

    private void deleteInIoExecutor(File f) {
        ioExecutor.submit(() -> {
            log.info("delete file: {}", f.getPath());
            if (!f.delete()) {
                log.error("delete file failed: {}", f.getPath());
            }
        });
    }

    @Override
    public CompletableFuture<Long> updateSnapshot(StateMachine<?, ?, ?> stateMachine,
                                                  Supplier<Boolean> cancelIndicator) {
        return new SnapshotSaveTask(stateMachine, cancelIndicator).exec();
    }

    private class SnapshotSaveTask {
        private final StateMachine<?, ?, ?> stateMachine;
        private final Supplier<Boolean> cancelIndicator;
        private final long startTime = System.currentTimeMillis();
        private final CompletableFuture<Long> future = new CompletableFuture<>();

        private String baseName;
        private File newDataFile;

        private AsynchronousFileChannel channel;
        private Snapshot currentSnapshot;
        private SnapshotIterator currentIterator;
        private AsyncIoTask writeTask;
        private long writePos;

        public SnapshotSaveTask(StateMachine<?, ?, ?> stateMachine, Supplier<Boolean> cancelIndicator) {
            this.stateMachine = stateMachine;
            this.cancelIndicator = cancelIndicator;
        }

        public CompletableFuture<Long> exec() {
            groupConfig.getRaftExecutor().execute(this::updateSnapshot);
            return future;
        }

        private void updateSnapshot() {
            // currentSaveTask should access in raft thread
            if (currentSaveTask != null) {
                log.warn("snapshot save task is running");
                future.complete(-1L);
                return;
            }
            currentSaveTask = this;
            try {
                currentSnapshot = stateMachine.takeSnapshot();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                baseName = sdf.format(new Date()) + "_" + currentSnapshot.getId();
                newDataFile = new File(snapshotDir, baseName + DATA_SUFFIX);
                currentIterator = currentSnapshot.openIterator();

                writeTask = new AsyncIoTask(channel, cancelIndicator);

                HashSet<StandardOpenOption> options = new HashSet<>();
                options.add(StandardOpenOption.CREATE_NEW);
                options.add(StandardOpenOption.WRITE);
                channel = AsynchronousFileChannel.open(newDataFile.toPath(), options, ioExecutor);

                read();
            } catch (Throwable e) {
                log.error("update snapshot failed", e);
                future.completeExceptionally(e);
                reset();
            }
        }

        private void read() {
            // read next should be called in raft thread
            currentIterator.readNext().whenCompleteAsync(
                    this::whenReadFinish,
                    groupConfig.getRaftExecutor());
        }

        private boolean shouldReturn(Throwable ex) {
            if (cancelIndicator.get()) {
                future.cancel(false);
                reset();
                return true;
            }
            if (ex != null) {
                future.completeExceptionally(ex);
                reset();
                return true;
            }
            return false;
        }

        private void whenReadFinish(RefBuffer rb, Throwable ex) {
            try {
                if (ex != null) {
                    log.error("read snapshot fail", ex);
                }
                if (shouldReturn(ex)) {
                    return;
                }
                if (rb != null && rb.getBuffer().hasRemaining()) {
                    CompletableFuture<Void> f = writeTask.write(false, false, rb.getBuffer(), writePos);
                    long bytes = rb.getBuffer().remaining();
                    f.whenCompleteAsync((v, writeEx) -> whenWriteFinish(rb, writeEx, bytes), groupConfig.getRaftExecutor());
                } else {
                    ioExecutor.submit(this::afterDataWriteFinish);
                }
            } catch (Throwable unexpect) {
                BugLog.log(unexpect);
                future.completeExceptionally(unexpect);
                reset();
            }
        }

        private void whenWriteFinish(RefBuffer rb, Throwable ex, long writeBytes) {
            try {
                rb.release();
                if (ex != null) {
                    log.error("write snapshot fail", ex);
                }
                if (shouldReturn(ex)) {
                    return;
                }
                writePos += writeBytes;
                read();
            } catch (Throwable unexpect) {
                BugLog.log(unexpect);
                future.completeExceptionally(unexpect);
                reset();
            }
        }

        // run in io thread
        private void afterDataWriteFinish() {
            try {
                if (shouldReturn(null)) {
                    return;
                }
                channel.force(true);
                if (shouldReturn(null)) {
                    return;
                }
                File idxfile = new File(snapshotDir, baseName + DATA_SUFFIX);
                try (StatusFile sf = new StatusFile(idxfile)) {
                    sf.init();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
                    sf.getProperties().setProperty(KEY_LAST_INDEX, String.valueOf(currentSnapshot.getLastIncludedIndex()));
                    sf.getProperties().setProperty(KEY_LAST_TERM, String.valueOf(currentSnapshot.getLastIncludedTerm()));
                    sf.getProperties().setProperty(KEY_SAVE_START_TIME, sdf.format(new Date(startTime)));
                    sf.getProperties().setProperty(KEY_SAVE_END_TIME, sdf.format(new Date()));
                    if (!sf.update()) {
                        future.completeExceptionally(new IOException("update status file fail"));
                        return;
                    }
                }
                deleteInIoExecutor(idxfile);
                deleteInIoExecutor(lastDataFile);
                future.complete(currentSnapshot.getLastIncludedIndex());
            } catch (Throwable ex) {
                log.error("finish save snapshot fail", ex);
                future.completeExceptionally(ex);
            } finally {
                reset();
            }
        }

        private void reset() {
            currentSaveTask = null;
        }
    }

}
