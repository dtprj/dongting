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
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftCancelException;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class DefaultSnapshotManager implements SnapshotManager {

    private static final DtLog log = DtLogs.getLogger(DefaultSnapshotManager.class);

    public static final String SNAPSHOT_DIR = "snapshot";

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
    private static final String KEY_NEXT_ID = "nextSnapshotId";

    private final RaftGroupConfigEx groupConfig;
    private final ExecutorService ioExecutor;
    private final RaftStatusImpl raftStatus;
    private final StateMachine stateMachine;
    private final Consumer<Long> logDeleter;

    private final SaveSnapshotLoopFrame saveLoopFrame;

    private long nextId = 1;
    private File snapshotDir;

    private static class FileSnapshotInfo {
        final File idxFile;
        final File dataFile;

        long lastIncludeIndex;

        SnapshotInfo si;

        FileSnapshotInfo(File idxFile, File dataFile) {
            this.idxFile = idxFile;
            this.dataFile = dataFile;
        }
    }

    private final LinkedList<FileSnapshotInfo> savedSnapshots = new LinkedList<>();
    private final LinkedList<Pair<Long, FiberFuture<Long>>> saveRequest = new LinkedList<>();

    public DefaultSnapshotManager(RaftGroupConfigEx groupConfig, StateMachine stateMachine, Consumer<Long> logDeleter) {
        this.groupConfig = groupConfig;
        this.ioExecutor = groupConfig.blockIoExecutor;
        this.raftStatus = (RaftStatusImpl) groupConfig.raftStatus;
        this.stateMachine = stateMachine;
        this.logDeleter = logDeleter;
        this.saveLoopFrame = new SaveSnapshotLoopFrame();
    }

    @Override
    public FiberFrame<Snapshot> init() {
        return new InitFrame();
    }

    private class InitFrame extends FiberFrame<Snapshot> {

        private StatusFile currentStatusFile;
        private int lastBufferSize;

        @Override
        public FrameCallResult execute(Void input) throws Exception {
            File dataDir = FileUtil.ensureDir(groupConfig.dataDir);
            snapshotDir = FileUtil.ensureDir(dataDir, SNAPSHOT_DIR);
            File[] files = snapshotDir.listFiles(f -> f.isFile() && f.getName().endsWith(IDX_SUFFIX));
            if (files == null || files.length == 0) {
                setResult(null);
                return Fiber.frameReturn();
            }
            Arrays.sort(files);
            for (int i = files.length - 1; i >= 0; i--) {
                File f = files[i];
                String baseName = FileUtil.baseName(f);
                File dataFile = new File(snapshotDir, baseName + DATA_SUFFIX);
                if (f.length() == 0) {
                    log.warn("empty status file: {}", f.getPath());
                    deleteInIoExecutor(f);
                    deleteInIoExecutor(dataFile);
                    continue;
                }
                if (dataFile.exists()) {
                    FileSnapshotInfo fsi = new FileSnapshotInfo(f, dataFile);
                    savedSnapshots.addFirst(fsi);
                } else {
                    log.error("missing data file: {}", f.getPath());
                    deleteInIoExecutor(f);
                }
            }
            if (savedSnapshots.isEmpty()) {
                log.warn("no saved snapshot found");
                setResult(null);
                return Fiber.frameReturn();
            }

            return loadIdxInfo(savedSnapshots.iterator());
        }

        private FrameCallResult loadIdxInfo(Iterator<FileSnapshotInfo> it) throws Exception {
            if (it.hasNext()) {
                FileSnapshotInfo fsi = it.next();
                log.info("load snapshot info: {}", fsi.idxFile);

                currentStatusFile = new StatusFile(fsi.idxFile, groupConfig);

                return Fiber.call(currentStatusFile.init(), v -> afterStatusFileInit(it, fsi));
            } else {
                FileSnapshotInfo last = savedSnapshots.getLast();
                log.info("open snapshot file {}", last.dataFile);
                FileSnapshot s = new FileSnapshot(groupConfig, last.si, last.dataFile, lastBufferSize);
                setResult(s);
                return Fiber.frameReturn();
            }
        }

        private FrameCallResult afterStatusFileInit(Iterator<FileSnapshotInfo> it, FileSnapshotInfo fsi) throws Exception {
            Map<String, String> p = currentStatusFile.getProperties();
            fsi.lastIncludeIndex = Long.parseLong(p.get(KEY_LAST_INDEX));

            if (!it.hasNext()) {
                nextId = Long.parseLong(p.get(KEY_NEXT_ID));
                lastBufferSize = Integer.parseInt(p.get(KEY_BUFFER_SIZE));

                int lastTerm = Integer.parseInt(p.get(KEY_LAST_TERM));
                Set<Integer> members = RaftUtil.strToIdSet(p.get(KEY_MEMBERS));
                Set<Integer> observers = RaftUtil.strToIdSet(p.get(KEY_OBSERVERS));
                Set<Integer> preparedMembers = RaftUtil.strToIdSet(p.get(KEY_PREPARED_MEMBERS));
                Set<Integer> preparedObservers = RaftUtil.strToIdSet(p.get(KEY_PREPARED_OBSERVERS));
                long lastConfigChangeIndex = Long.parseLong(p.get(KEY_LAST_CONFIG_CHANGE_INDEX));

                fsi.si = new SnapshotInfo(fsi.lastIncludeIndex, lastTerm, members, observers, preparedMembers,
                        preparedObservers, lastConfigChangeIndex);
            }

            currentStatusFile = null;

            return loadIdxInfo(it);
        }
    }

    @Override
    public FiberFrame<Void> recover(Snapshot snapshot) {
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
        Fiber f = new Fiber("save-snapshot-" + groupConfig.groupId, groupConfig.fiberGroup,
                saveLoopFrame, true);
        f.start();
    }

    @Override
    public void stopFiber() {
        saveLoopFrame.stopLoop = true;
        saveLoopFrame.saveSnapshotCond.signal();
    }

    class SaveSnapshotLoopFrame extends FiberFrame<Void> {

        final FiberCondition saveSnapshotCond;
        private boolean stopLoop;

        @Override
        protected FrameCallResult handle(Throwable ex) throws Throwable {
            throw Fiber.fatal(ex);
        }

        SaveSnapshotLoopFrame() {
            this.saveSnapshotCond = groupConfig.fiberGroup.newCondition("saveSnapshotLoop");
        }

        @Override
        public FrameCallResult execute(Void input) throws Throwable {
            if (stopLoop) {
                return Fiber.frameReturn();
            }
            deleteOldFiles();
            if (saveRequest.isEmpty()) {
                return saveSnapshotCond.await(groupConfig.saveSnapshotSeconds * 1000L, this::doSave);
            } else {
                return doSave(null);
            }
        }

        private FrameCallResult doSave(Void unused) {
            if (stopLoop) {
                return Fiber.frameReturn();
            }
            SaveFrame f = new SaveFrame(nextId++);
            return Fiber.call(f, this::afterSave);
        }

        private FrameCallResult afterSave(Void v) {
            deleteOldFiles();
            if (!isGroupShouldStopPlain() && groupConfig.deleteLogsAfterTakeSnapshot && !savedSnapshots.isEmpty()) {
                long lastIncludeIndex = savedSnapshots.getFirst().lastIncludeIndex;
                if (lastIncludeIndex > 0 && logDeleter != null) {
                    logDeleter.accept(lastIncludeIndex);
                }
            }
            return Fiber.resume(null, this);
        }

        private void deleteOldFiles() {
            int keep = Math.max(1, groupConfig.maxKeepSnapshots);
            while (savedSnapshots.size() > keep) {
                FileSnapshotInfo s = savedSnapshots.removeFirst();
                deleteInIoExecutor(s.dataFile);
                deleteInIoExecutor(s.idxFile);
            }
        }
    }

    @Override
    public FiberFuture<Long> saveSnapshot() {
        FiberFuture<Long> f = groupConfig.fiberGroup.newFuture("saveSnapshot-" + groupConfig.groupId);
        saveRequest.addLast(new Pair<>(raftStatus.getLastApplied(), f));
        saveLoopFrame.saveSnapshotCond.signal();
        return f;
    }

    private class SaveFrame extends FiberFrame<Void> {
        private final long startTime = System.currentTimeMillis();

        private final CRC32C crc32c = new CRC32C();

        private final SnapshotInfo snapshotInfo = new SnapshotInfo(raftStatus);

        private final int bufferSize = groupConfig.diskSnapshotBufferSize;
        private final long id;
        private RefBufferFactory directBufferFactory;


        private DtFile newDataFile;

        private File newIdxFile;
        private StatusFile statusFile;

        private Snapshot readSnapshot;
        private FileSnapshotInfo fileSnapshot;

        private long currentWritePos;

        private boolean success;
        private boolean cancel;

        public SaveFrame(long id) {
            this.id = id;
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            if (ex instanceof RaftCancelException) {
                log.warn("save snapshot task is cancelled");
            } else {
                log.error("save snapshot failed", ex);
            }
            complete(ex);
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult doFinally() {
            if (newDataFile != null && newDataFile.getChannel() != null) {
                DtUtil.close(newDataFile.getChannel());
            }
            if (readSnapshot != null) {
                readSnapshot.close();
            }
            if (!success) {
                if (cancel) {
                    complete(new RaftCancelException("save snapshot task is cancelled"));
                }
                if (newDataFile != null) {
                    deleteInIoExecutor(newDataFile.getFile());
                }
                if (newIdxFile != null) {
                    deleteInIoExecutor(newIdxFile);
                }
            } else {
                complete(null);
            }
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (checkCancel()) {
                return Fiber.frameReturn();
            }
            this.directBufferFactory = new RefBufferFactory(getFiberGroup().dispatcher.thread.directPool, 0);
            FiberFuture<Snapshot> f =stateMachine.takeSnapshot(snapshotInfo);
            return f.await(this::afterTakeSnapshot);
        }

        private FrameCallResult afterTakeSnapshot(Snapshot snapshot) throws Exception {
            this.readSnapshot = snapshot;
            log.info("begin save snapshot {}. groupId={}, lastIndex={}, lastTerm={}", id,
                    groupConfig.groupId, snapshotInfo.lastIncludedIndex, snapshotInfo.lastIncludedTerm);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String baseName = sdf.format(new Date()) + "_" + id;
            File dataFile = new File(snapshotDir, baseName + DATA_SUFFIX);
            this.newIdxFile = new File(snapshotDir, baseName + IDX_SUFFIX);

            HashSet<StandardOpenOption> options = new HashSet<>();
            options.add(StandardOpenOption.CREATE_NEW);
            options.add(StandardOpenOption.WRITE);
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(dataFile.toPath(), options,
                    getFiberGroup().getExecutor());
            this.newDataFile = new DtFile(dataFile, channel, groupConfig.fiberGroup);

            int readConcurrency = groupConfig.snapshotConcurrency;
            int writeConcurrency = groupConfig.diskSnapshotConcurrency;
            SnapshotReader reader = new SnapshotReader(readSnapshot, readConcurrency, writeConcurrency,
                    this::writeCallback, this::checkCancel, this::createBuffer);
            return Fiber.call(reader, this::finishDataFile);
        }

        private RefBuffer createBuffer() {
            RefBuffer buf = directBufferFactory.create(bufferSize);
            buf.getBuffer().position(4);
            buf.getBuffer().limit(bufferSize - 4);
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
            AsyncIoTask writeTask = new AsyncIoTask(groupConfig.fiberGroup, newDataFile);
            long newWritePos = currentWritePos + buf.capacity();
            FiberFuture<Void> writeFuture = writeTask.write(buf, currentWritePos);
            currentWritePos = newWritePos;
            writeFuture.registerCallback((v, ex) -> rb.release());
            return writeFuture;
        }

        private boolean checkCancel() {
            // do not check isGroupShouldStopPlain() here

            if (raftStatus.installSnapshot) {
                if (!cancel) {
                    log.warn("install snapshot, cancel save snapshot task");
                    cancel = true;
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
            log.info("snapshot {} data file write success: {}", id, newDataFile.getFile().getPath());

            statusFile = new StatusFile(newIdxFile, groupConfig);
            return Fiber.call(statusFile.init(), this::saveIdxFile);
        }

        private FrameCallResult saveIdxFile(Void unused) {
            SnapshotInfo si = readSnapshot.getSnapshotInfo();
            Map<String, String> p = statusFile.getProperties();
            p.put(KEY_LAST_INDEX, String.valueOf(si.lastIncludedIndex));
            p.put(KEY_LAST_TERM, String.valueOf(si.lastIncludedTerm));
            p.put(KEY_MEMBERS, RaftUtil.setToStr(si.members));
            p.put(KEY_OBSERVERS, RaftUtil.setToStr(si.observers));
            p.put(KEY_PREPARED_MEMBERS, RaftUtil.setToStr(si.preparedMembers));
            p.put(KEY_PREPARED_OBSERVERS, RaftUtil.setToStr(si.preparedObservers));
            p.put(KEY_LAST_CONFIG_CHANGE_INDEX, String.valueOf(si.lastConfigChangeIndex));
            p.put(KEY_BUFFER_SIZE, String.valueOf(bufferSize));
            p.put(KEY_NEXT_ID, String.valueOf(id + 1));

            fileSnapshot = new FileSnapshotInfo(newIdxFile, newDataFile.getFile());
            fileSnapshot.lastIncludeIndex = snapshotInfo.lastIncludedIndex;

            // just for human reading
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            statusFile.getProperties().put("saveStartTime", sdf.format(new Date(startTime)));
            statusFile.getProperties().put("saveEndTime", sdf.format(new Date()));

            return statusFile.update().await(this::finish2);
        }

        private FrameCallResult finish2(Void unused) {
            success = true;
            log.info("snapshot status file write success: {}", newIdxFile.getPath());
            savedSnapshots.addLast(fileSnapshot);
            raftStatus.lastSavedSnapshotIndex = snapshotInfo.lastIncludedIndex;

            return Fiber.frameReturn();
        }

        private void complete(Throwable ex) {
            long raftIndex = snapshotInfo.lastIncludedIndex;
            Pair<Long, FiberFuture<Long>> req;
            while ((req = saveRequest.peek()) != null) {
                if (req.getLeft() <= raftIndex) {
                    if (ex == null) {
                        req.getRight().complete(raftIndex);
                    } else {
                        req.getRight().completeExceptionally(ex);
                    }
                    saveRequest.removeFirst();
                } else {
                    break;
                }
            }
        }
    }

}

class RecoverFiberFrame extends FiberFrame<Void> {

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
        ByteBufferPool p = groupConfig.fiberGroup.dispatcher.thread.directPool;
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
        int readConcurrency = groupConfig.diskSnapshotConcurrency;
        int writeConcurrency = groupConfig.snapshotConcurrency;
        SnapshotReader reader = new SnapshotReader(snapshot, readConcurrency, writeConcurrency, this::apply,
                this::isGroupShouldStopPlain, bufferCreator);
        return Fiber.call(reader, this::finish);
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
        FiberFuture<Void> f = stateMachine.installSnapshot(si.lastIncludedIndex, si.lastIncludedTerm,
                offset, false, buf);
        offset += size;
        f.registerCallback((v, ex) -> rb.release());
        return f;
    }

    private FrameCallResult finish(Void v) {
        SnapshotInfo si = snapshot.getSnapshotInfo();
        FiberFuture<Void> f = stateMachine.installSnapshot(si.lastIncludedIndex, si.lastIncludedTerm,
                offset, true, SimpleByteBufferPool.EMPTY_BUFFER);
        return f.await(this::justReturn);
    }

}
