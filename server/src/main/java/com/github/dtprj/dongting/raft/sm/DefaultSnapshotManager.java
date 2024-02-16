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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCall;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftStatus;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;
import com.github.dtprj.dongting.raft.store.DtFile;
import com.github.dtprj.dongting.raft.store.StatusFile;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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

    private final RaftGroupConfigEx groupConfig;
    private final ExecutorService ioExecutor;
    private final RaftStatus raftStatus;

    private File snapshotDir;

    private File lastIdxFile;
    private File lastDataFile;

    private SnapshotSaveTask currentSaveTask;

    public DefaultSnapshotManager(RaftGroupConfigEx groupConfig, ExecutorService ioExecutor) {
        this.groupConfig = groupConfig;
        this.ioExecutor = ioExecutor;
        this.raftStatus = groupConfig.getRaftStatus();
    }

    @Override
    public FiberFrame<Snapshot> init() {
        return new InitFrame();
    }

    private class InitFrame extends FiberFrame<Snapshot> {

        private StatusFile snapshotIdxFile;

        @Override
        public FrameCallResult execute(Void input) {
            File dataDir = FileUtil.ensureDir(groupConfig.getDataDir());
            snapshotDir = FileUtil.ensureDir(dataDir, "snapshot");
            File[] files = snapshotDir.listFiles(f -> f.isFile() &&
                    (f.getName().endsWith(DATA_SUFFIX) || f.getName().endsWith(IDX_SUFFIX)));
            if (files == null || files.length == 0) {
                setResult(null);
                return Fiber.frameReturn();
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
                setResult(null);
                return Fiber.frameReturn();
            } else {
                log.info("use snapshot: {}", lastDataFile.getPath());
            }

            for (File f : files) {
                if (f != lastIdxFile && f != lastDataFile) {
                    deleteInIoExecutor(f);
                }
            }

            this.snapshotIdxFile = new StatusFile(lastDataFile, ioExecutor, getFiberGroup());

            return Fiber.call(snapshotIdxFile.init(), this::afterStatusFileInit);
        }

        private FrameCallResult afterStatusFileInit(Void unused) throws Exception {
            String lastIndex = snapshotIdxFile.getProperties().getProperty(KEY_LAST_INDEX);
            String lastTerm = snapshotIdxFile.getProperties().getProperty(KEY_LAST_TERM);

            FileSnapshot s = new FileSnapshot(groupConfig, Long.parseLong(lastIndex),
                    Integer.parseInt(lastTerm), lastDataFile, ioExecutor);
            log.info("open snapshot file {}", lastDataFile);
            setResult(s);
            return Fiber.frameReturn();
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
    public void saveSnapshot(StateMachine stateMachine, CompletableFuture<Long> result) {
        if (currentSaveTask != null) {
            log.warn("snapshot save task is running");
            result.complete(-1L);
            return;
        }
        currentSaveTask = new SnapshotSaveTask(stateMachine, result);
        Fiber f = new Fiber("saveSnapshot", FiberGroup.currentGroup(), currentSaveTask);
        f.start();
    }

    private class SnapshotSaveTask extends FiberFrame<Void> {
        private final StateMachine stateMachine;
        private final CompletableFuture<Long> result;
        private final long startTime = System.currentTimeMillis();
        private final CompletableFuture<Long> future = new CompletableFuture<>();
        private final CRC32C crc32c = new CRC32C();
        private final ByteBuffer headerBuffer = ByteBuffer.allocate(8);

        private DtFile newDataFile;

        private File newIdxFile;
        private StatusFile statusFile;

        private Snapshot readSnapshot;
        private RefBuffer readBuffer;

        private long currentWritePos;

        public SnapshotSaveTask(StateMachine stateMachine, CompletableFuture<Long> result) {
            this.stateMachine = stateMachine;
            this.result = result;
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            result.completeExceptionally(ex);
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult doFinally() {
            currentSaveTask = null;
            releaseReadBuffer();
            if (newDataFile != null && newDataFile.getChannel() != null) {
                DtUtil.close(newDataFile.getChannel());
            }
            DtUtil.close(statusFile);
            if (readSnapshot != null) {
                readSnapshot.close();
            }
            if (future.isCompletedExceptionally() || future.isCancelled()) {
                if (newDataFile != null) {
                    deleteInIoExecutor(newDataFile.getFile());
                }
                deleteInIoExecutor(newIdxFile);
            }
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
            return Fiber.call(stateMachine.takeSnapshot(raftStatus.getCurrentTerm()), this::afterTakeSnapshot);
        }

        private FrameCallResult afterTakeSnapshot(Snapshot snapshot) throws Exception {
            readSnapshot = snapshot;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String baseName = sdf.format(new Date()) + "_" + readSnapshot.getId();
            File dataFile = new File(snapshotDir, baseName + DATA_SUFFIX);
            this.newIdxFile = new File(snapshotDir, baseName + IDX_SUFFIX);

            HashSet<StandardOpenOption> options = new HashSet<>();
            options.add(StandardOpenOption.CREATE_NEW);
            options.add(StandardOpenOption.WRITE);
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(dataFile.toPath(), options, ioExecutor);
            this.newDataFile = new DtFile(dataFile, channel, groupConfig.getFiberGroup());

            return read();
        }

        private boolean checkCancel() {
            if (future.isCancelled()) {
                log.info("snapshot save task is cancelled");
                return true;
            }
            if (isGroupShouldStopPlain()) {
                log.info("snapshot save task is cancelled");
                future.cancel(false);
                return true;
            }
            return false;
        }

        private FrameCallResult read() {
            if (checkCancel()) {
                return Fiber.frameReturn();
            }
            FiberFuture<RefBuffer> fu = readSnapshot.readNext();
            return fu.await(this::whenReadFinish);
        }

        private FrameCallResult write(ByteBuffer buf, FrameCall<Void> resumePoint) {
            AsyncIoTask writeTask = new AsyncIoTask(getFiberGroup(), newDataFile);
            long newWritePos = currentWritePos + buf.remaining();
            FiberFuture<Void> writeFuture = writeTask.write(buf, currentWritePos);
            currentWritePos = newWritePos;
            return writeFuture.await(resumePoint);
        }

        private FrameCallResult whenReadFinish(RefBuffer rb) throws Exception {
            this.readBuffer = rb;
            if (checkCancel()) {
                return Fiber.frameReturn();
            }
            if (rb != null && rb.getBuffer() != null && rb.getBuffer().hasRemaining()) {
                ByteBuffer buffer = rb.getBuffer();
                crc32c.reset();
                RaftUtil.updateCrc(crc32c, buffer, buffer.position(), buffer.remaining());
                headerBuffer.clear();
                headerBuffer.putInt(buffer.remaining());
                headerBuffer.putInt((int) crc32c.getValue());
                return write(headerBuffer, this::whenHeaderWriteFinish);
            } else {
                return writeIdxFile();
            }
        }

        private FrameCallResult whenHeaderWriteFinish(Void unused) {
            if (checkCancel()) {
                return Fiber.frameReturn();
            }
            return write(readBuffer.getBuffer(), this::whenWriteFinish);
        }

        private FrameCallResult whenWriteFinish(Void unused) {
            if (checkCancel()) {
                return Fiber.frameReturn();
            }
            releaseReadBuffer();
            return read();
        }

        private FrameCallResult writeIdxFile() throws Exception {
            if (checkCancel()) {
                return Fiber.frameReturn();
            }
            //TODO block here
            newDataFile.getChannel().force(true);
            log.info("snapshot data file write success: {}", newDataFile.getFile().getPath());
            if (checkCancel()) {
                return Fiber.frameReturn();
            }

            statusFile = new StatusFile(newIdxFile, ioExecutor, getFiberGroup());
            statusFile.init();
            statusFile.getProperties().setProperty(KEY_LAST_INDEX, String.valueOf(readSnapshot.getLastIncludedIndex()));
            statusFile.getProperties().setProperty(KEY_LAST_TERM, String.valueOf(readSnapshot.getLastIncludedTerm()));

            // just for human reading
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            statusFile.getProperties().setProperty("saveStartTime", sdf.format(new Date(startTime)));
            statusFile.getProperties().setProperty("saveEndTime", sdf.format(new Date()));

            return statusFile.update(true).await(this::finish2);
        }

        private FrameCallResult finish2(Void unused) {
            log.info("snapshot status file write success: {}", newIdxFile.getPath());

            future.complete(readSnapshot.getLastIncludedIndex());

            File oldIdxFile = lastIdxFile;
            File oldDataFile = lastDataFile;
            lastIdxFile = newIdxFile;
            lastDataFile = newDataFile.getFile();
            deleteInIoExecutor(oldIdxFile);
            deleteInIoExecutor(oldDataFile);
            return Fiber.frameReturn();
        }

        private void releaseReadBuffer() {
            if (readBuffer != null) {
                readBuffer.release();
                readBuffer = null;
            }
        }
    }

}
