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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCancelException;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.SnapshotReader;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;
import com.github.dtprj.dongting.raft.store.DtFile;
import com.github.dtprj.dongting.raft.store.ForceFrame;
import com.github.dtprj.dongting.raft.store.StatusFile;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class DefaultSnapshotManager implements SnapshotManager {

    private static final DtLog log = DtLogs.getLogger(DefaultSnapshotManager.class);

    private static final int KEEP = 2;

    private static final String DATA_SUFFIX = ".data";
    private static final String IDX_SUFFIX = ".idx";

    private static final String KEY_LAST_INDEX = "lastIncludedIndex";
    private static final String KEY_LAST_TERM = "lastIncludedTerm";
    private static final String KEY_MEMBERS = "members";
    private static final String KEY_OBSERVERS = "observers";
    private static final String KEY_PREPARED_MEMBERS = "preparedMembers";
    private static final String KEY_PREPARED_OBSERVERS = "preparedObservers";
    private static final String KEY_LAST_CONFIG_CHANGE_INDEX = "lastConfigChangeIndex";
    private static final String KEY_BUFFER_SIZE = "bufferSize";

    private final RaftGroupConfigEx groupConfig;
    private final ExecutorService ioExecutor;
    private final RaftStatusImpl raftStatus;
    private final StateMachine stateMachine;

    private final SaveSnapshotLoopFrame saveLoopFrame;

    private File snapshotDir;

    private final LinkedList<Pair<File, File>> snapshotFiles = new LinkedList<>();

    public DefaultSnapshotManager(RaftGroupConfigEx groupConfig, StateMachine stateMachine) {
        this.groupConfig = groupConfig;
        this.ioExecutor = groupConfig.getBlockIoExecutor();
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.stateMachine = stateMachine;
        this.saveLoopFrame = new SaveSnapshotLoopFrame();
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
            File[] files = snapshotDir.listFiles(f -> f.isFile() && f.getName().endsWith(IDX_SUFFIX));
            if (files == null || files.length == 0) {
                setResult(null);
                return Fiber.frameReturn();
            }
            Arrays.sort(files);
            for (int i = files.length - 1; i >= 0; i--) {
                File f = files[i];
                if (f.length() == 0) {
                    deleteInIoExecutor(f);
                    continue;
                }
                String baseName = FileUtil.baseName(f);
                File dataFile = new File(snapshotDir, baseName + DATA_SUFFIX);
                if (dataFile.exists()) {
                    snapshotFiles.addFirst(new Pair<>(f, dataFile));
                } else {
                    log.error("missing data file: {}", f.getPath());
                    deleteInIoExecutor(f);
                }
            }
            if (snapshotFiles.isEmpty()) {
                log.info("no saved snapshot found");
                setResult(null);
                return Fiber.frameReturn();
            }

            Pair<File, File> last = snapshotFiles.getLast();
            log.info("use snapshot: {}", last.getRight());

            this.snapshotIdxFile = new StatusFile(last.getLeft(), groupConfig);

            return Fiber.call(snapshotIdxFile.init(), v -> afterStatusFileInit(last));
        }

        private FrameCallResult afterStatusFileInit(Pair<File, File> last) throws Exception {
            Map<String, String> p = snapshotIdxFile.getProperties();
            long lastIndex = Long.parseLong(p.get(KEY_LAST_INDEX));
            int lastTerm = Integer.parseInt(p.get(KEY_LAST_TERM));
            Set<Integer> members = RaftUtil.strToIdSet(p.get(KEY_MEMBERS));
            Set<Integer> observers = RaftUtil.strToIdSet(p.get(KEY_OBSERVERS));
            Set<Integer> preparedMembers = RaftUtil.strToIdSet(p.get(KEY_PREPARED_MEMBERS));
            Set<Integer> preparedObservers = RaftUtil.strToIdSet(p.get(KEY_PREPARED_OBSERVERS));
            long lastConfigChangeIndex = Long.parseLong(p.get(KEY_LAST_CONFIG_CHANGE_INDEX));
            int bufferSize = Integer.parseInt(p.get(KEY_BUFFER_SIZE));
            SnapshotInfo si = new SnapshotInfo(lastIndex, lastTerm, members, observers, preparedMembers,
                    preparedObservers, lastConfigChangeIndex);

            FileSnapshot s = new FileSnapshot(groupConfig, si, last.getRight(), bufferSize);
            log.info("open snapshot file {}", last.getRight());
            setResult(s);
            return Fiber.frameReturn();
        }
    }

    @Override
    public FiberFrame<Pair<Integer, Long>> recover(Snapshot snapshot) {
        return new RecoverFiberFrame(groupConfig, stateMachine, (FileSnapshot) snapshot);
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
    public void startFiber() {
        Fiber f = new Fiber("save-snapshot-" + groupConfig.getGroupId(), groupConfig.getFiberGroup(),
                saveLoopFrame, true);
        f.start();
    }

    class SaveSnapshotLoopFrame extends FiberFrame<Void> {

        final FiberCondition saveSnapshotCond;
        final LinkedList<Pair<Long, CompletableFuture<Long>>> saveRequest = new LinkedList<>();

        @Override
        protected FrameCallResult handle(Throwable ex) throws Throwable {
            throw Fiber.fatal(ex);
        }

        SaveSnapshotLoopFrame() {
            this.saveSnapshotCond = groupConfig.getFiberGroup().newCondition("saveSnapshotLoop");
        }

        @Override
        public FrameCallResult execute(Void input) throws Throwable {
            deleteOldFiles();
            if (saveRequest.isEmpty()) {
                return saveSnapshotCond.await(groupConfig.getSaveSnapshotMillis(), this::doSave);
            } else {
                return doSave(null);
            }
        }

        private FrameCallResult doSave(Void unused) {
            SaveFrame f = new SaveFrame();
            return Fiber.call(f, v -> afterSave(f));
        }

        private FrameCallResult afterSave(SaveFrame f) {
            long raftIndex = f.readSnapshot.getSnapshotInfo().getLastIncludedIndex();
            Pair<Long, CompletableFuture<Long>> req;
            while ((req = saveRequest.peek()) != null) {
                if (req.getLeft() <= raftIndex) {
                    req.getRight().complete(raftIndex);
                    saveRequest.removeFirst();
                } else {
                    break;
                }
            }
            deleteOldFiles();
            return Fiber.resume(null, this);
        }

        private void deleteOldFiles() {
            while (snapshotFiles.size() > KEEP) {
                Pair<File, File> p = snapshotFiles.removeFirst();
                deleteInIoExecutor(p.getLeft());
                deleteInIoExecutor(p.getRight());
            }
        }
    }

    @Override
    public void fireSaveSnapshot(CompletableFuture<Long> f) {
        saveLoopFrame.saveRequest.addLast(new Pair<>(raftStatus.getLastApplied(), f));
        saveLoopFrame.saveSnapshotCond.signal();
    }

    private class SaveFrame extends FiberFrame<Void> {
        private final long startTime = System.currentTimeMillis();

        private final CRC32C crc32c = new CRC32C();

        private final int bufferSize = groupConfig.getDiskSnapshotBufferSize();
        private RefBufferFactory directBufferFactory;


        private DtFile newDataFile;

        private File newIdxFile;
        private StatusFile statusFile;

        private Snapshot readSnapshot;

        private long currentWritePos;

        private boolean success;
        private boolean logCancel;

        @Override
        protected FrameCallResult handle(Throwable ex) {
            if (ex instanceof FiberCancelException) {
                log.warn("save snapshot task is cancelled");
            } else {
                log.error("save snapshot failed", ex);
            }
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult doFinally() {
            if (newDataFile != null && newDataFile.getChannel() != null) {
                DtUtil.close(newDataFile.getChannel());
            }
            DtUtil.close(statusFile);
            if (readSnapshot != null) {
                readSnapshot.close();
            }
            if (!success) {
                if (newDataFile != null) {
                    deleteInIoExecutor(newDataFile.getFile());
                }
                if (newIdxFile != null) {
                    deleteInIoExecutor(newIdxFile);
                }
            }
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) throws Exception {
            if (checkCancel()) {
                return Fiber.frameReturn();
            }
            this.directBufferFactory = new RefBufferFactory(getFiberGroup().getThread().getDirectPool(), 0);
            readSnapshot = stateMachine.takeSnapshot(new SnapshotInfo(raftStatus));

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String baseName = sdf.format(new Date()) + "_" + readSnapshot.getId();
            File dataFile = new File(snapshotDir, baseName + DATA_SUFFIX);
            this.newIdxFile = new File(snapshotDir, baseName + IDX_SUFFIX);

            HashSet<StandardOpenOption> options = new HashSet<>();
            options.add(StandardOpenOption.CREATE_NEW);
            options.add(StandardOpenOption.WRITE);
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(dataFile.toPath(), options,
                    getFiberGroup().getExecutor());
            this.newDataFile = new DtFile(dataFile, channel, groupConfig.getFiberGroup());

            int readConcurrency = groupConfig.getSnapshotConcurrency();
            int writeConcurrency = groupConfig.getDiskSnapshotConcurrency();
            SnapshotReader reader = new SnapshotReader(readSnapshot, readConcurrency, writeConcurrency,
                    this::writeCallback, this::checkCancel, this::createBuffer);
            return Fiber.call(reader, this::finishDataFile);
        }

        private RefBuffer createBuffer() {
            RefBuffer buf = directBufferFactory.create(bufferSize);
            buf.getBuffer().position(4);
            buf.getBuffer().limit(bufferSize - 8);
            return buf;
        }

        private FiberFuture<Void> writeCallback(RefBuffer rb, Integer readBytes) {
            crc32c.reset();
            ByteBuffer buf = rb.getBuffer();
            buf.clear();
            int size = readBytes;
            buf.putInt(0, size);
            RaftUtil.updateCrc(crc32c, buf, 0, size + 4);
            buf.putInt(size + 4, (int) crc32c.getValue());
            buf.position(0);
            buf.limit(size + 8);
            AsyncIoTask writeTask = new AsyncIoTask(groupConfig.getFiberGroup(), newDataFile);
            long newWritePos = currentWritePos + buf.capacity();
            FiberFuture<Void> writeFuture = writeTask.write(buf, currentWritePos);
            currentWritePos = newWritePos;
            writeFuture.registerCallback((v, ex) -> rb.release());
            return writeFuture;
        }

        private boolean checkCancel() {
            if (isGroupShouldStopPlain()) {
                if (logCancel) {
                    log.info("snapshot save task is cancelled");
                    logCancel = true;
                }
                return true;
            }
            if (raftStatus.isInstallSnapshot()) {
                if (logCancel) {
                    log.warn("install snapshot, cancel save snapshot task");
                    logCancel = true;
                }
                return true;
            }
            return false;
        }

        private FrameCallResult finishDataFile(Void v) {
            if (checkCancel()) {
                return Fiber.frameReturn();
            }
            ForceFrame ff = new ForceFrame(newDataFile.getChannel(), ioExecutor, true);
            return Fiber.call(ff, this::writeIdxFile);
        }

        private FrameCallResult writeIdxFile(Void v) {
            if (checkCancel()) {
                return Fiber.frameReturn();
            }
            log.info("snapshot data file write success: {}", newDataFile.getFile().getPath());

            statusFile = new StatusFile(newIdxFile, groupConfig);
            return Fiber.call(statusFile.init(), this::afterStatusFileInit);
        }

        private FrameCallResult afterStatusFileInit(Void unused) {
            SnapshotInfo si = readSnapshot.getSnapshotInfo();
            Map<String, String> p = statusFile.getProperties();
            p.put(KEY_LAST_INDEX, String.valueOf(si.getLastIncludedIndex()));
            p.put(KEY_LAST_TERM, String.valueOf(si.getLastIncludedTerm()));
            p.put(KEY_MEMBERS, RaftUtil.setToStr(si.getMembers()));
            p.put(KEY_OBSERVERS, RaftUtil.setToStr(si.getObservers()));
            p.put(KEY_PREPARED_MEMBERS, RaftUtil.setToStr(si.getPreparedMembers()));
            p.put(KEY_PREPARED_OBSERVERS, RaftUtil.setToStr(si.getPreparedObservers()));
            p.put(KEY_LAST_CONFIG_CHANGE_INDEX, String.valueOf(si.getLastConfigChangeIndex()));
            p.put(KEY_BUFFER_SIZE, String.valueOf(bufferSize));

            // just for human reading
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            statusFile.getProperties().put("saveStartTime", sdf.format(new Date(startTime)));
            statusFile.getProperties().put("saveEndTime", sdf.format(new Date()));

            return statusFile.update(true).await(this::finish2);
        }

        private FrameCallResult finish2(Void unused) {
            success = true;
            log.info("snapshot status file write success: {}", newIdxFile.getPath());
            snapshotFiles.addLast(new Pair<>(newIdxFile, newDataFile.getFile()));
            return Fiber.frameReturn();
        }
    }

}

class RecoverFiberFrame extends FiberFrame<Pair<Integer, Long>> {

    private final StateMachine stateMachine;
    private final RaftGroupConfigEx groupConfig;
    private final FileSnapshot snapshot;

    private final Supplier<RefBuffer> bufferCreator;

    private final CRC32C crc32C = new CRC32C();

    private long offset;

    public RecoverFiberFrame(RaftGroupConfigEx groupConfig, StateMachine stateMachine, FileSnapshot snapshot) {
        this.stateMachine = stateMachine;
        this.groupConfig = groupConfig;
        this.snapshot = snapshot;
        ByteBufferPool p = groupConfig.getFiberGroup().getThread().getDirectPool();
        RefBufferFactory f = new RefBufferFactory(p, 0);
        this.bufferCreator = () -> f.create(snapshot.getBufferSize());
    }

    @Override
    protected FrameCallResult doFinally() {
        if (snapshot != null) {
            snapshot.close();
        }
        return Fiber.frameReturn();
    }

    @Override
    public FrameCallResult execute(Void input) {
        int readConcurrency = groupConfig.getDiskSnapshotConcurrency();
        int writeConcurrency = groupConfig.getSnapshotConcurrency();
        SnapshotReader reader = new SnapshotReader(snapshot, readConcurrency, writeConcurrency, this::apply,
                this::isGroupShouldStopPlain, bufferCreator);
        return Fiber.call(reader, this::finish1);
    }

    private FiberFuture<Void> apply(RefBuffer rb, Integer notUsed) {
        ByteBuffer buf = rb.getBuffer();
        int size = buf.getInt(0);
        if (size <= 0 || size > buf.capacity() - 8) {
            rb.release();
            return FiberFuture.failedFuture(getFiberGroup(), new RaftException("invalid snapshot data size: " + size));
        }
        crc32C.reset();
        RaftUtil.updateCrc(crc32C, buf, 0, size + 4);
        int crc = buf.getInt(size + 4);
        if (crc != (int) crc32C.getValue()) {
            rb.release();
            return FiberFuture.failedFuture(getFiberGroup(), new RaftException("snapshot data crc error"));
        }
        buf.limit(size + 4);
        buf.position(4);
        SnapshotInfo si = snapshot.getSnapshotInfo();
        FiberFuture<Void> f = stateMachine.installSnapshot(si.getLastIncludedIndex(), si.getLastIncludedTerm(),
                offset, false, buf);
        offset += size;
        f.registerCallback((v, ex) -> rb.release());
        return f;
    }

    private FrameCallResult finish1(Void v) {
        SnapshotInfo si = snapshot.getSnapshotInfo();
        FiberFuture<Void> f = stateMachine.installSnapshot(si.getLastIncludedIndex(), si.getLastIncludedTerm(),
                offset, true, SimpleByteBufferPool.EMPTY_BUFFER);
        return f.await(this::finish2);
    }

    private FrameCallResult finish2(Void v) {
        SnapshotInfo si = snapshot.getSnapshotInfo();
        setResult(new Pair<>(si.getLastIncludedTerm(), si.getLastIncludedIndex()));
        return Fiber.frameReturn();
    }
}
