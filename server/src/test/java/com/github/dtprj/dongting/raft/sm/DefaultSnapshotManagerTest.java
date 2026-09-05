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

import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvReq;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.dtkv.server.DtKV;
import com.github.dtprj.dongting.dtkv.server.KvServerConfig;
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.server.DefaultRaftFactory;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.test.TestDir;
import com.github.dtprj.dongting.test.Tick;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class DefaultSnapshotManagerTest extends BaseFiberTest {
    private static RaftServer RAFT_SERVER;
    private DefaultSnapshotManager m;
    private DtKV kv;
    private RaftStatusImpl raftStatus;
    private RaftGroupConfigEx groupConfig;

    private void createManager(boolean separateExecutor, String dataDir, boolean mockInstall) {
        raftStatus = new RaftStatusImpl(1, dispatcher.ts);
        raftStatus.nodeIdOfMembers = Set.of(1);
        raftStatus.nodeIdOfObservers = Set.of();
        raftStatus.nodeIdOfPreparedMembers = Set.of();
        raftStatus.nodeIdOfPreparedObservers = Set.of();
        raftStatus.lastAppliedTerm = 1;
        groupConfig = new RaftGroupConfigEx(1, "1", "");
        groupConfig.fiberGroup = fiberGroup;
        groupConfig.raftStatus = raftStatus;
        groupConfig.ts = dispatcher.ts;
        groupConfig.dataDir = dataDir;
        groupConfig.blockIoExecutor = MockExecutors.ioExecutor();
        if (RAFT_SERVER == null) {
            RaftServerConfig sc = new RaftServerConfig();
            sc.servers = "1,localhost:4000";
            sc.nodeId = 1;
            sc.replicatePort = 4000;
            RAFT_SERVER = new RaftServer(sc, new ArrayList<>(), new DefaultRaftFactory(DefaultPoolFactory.INSTANCE) {
                @Override
                public StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
                    return null;
                }
            });
        }
        groupConfig.raftServer = RAFT_SERVER;
        KvServerConfig kvConfig = new KvServerConfig();
        kvConfig.useSeparateExecutor = separateExecutor;
        kvConfig.initMapCapacity = 16;
        kv = new DtKV(groupConfig, kvConfig) {
            @Override
            public FiberFuture<Snapshot> takeSnapshot(SnapshotInfo si) {
                if (mockInstall) {
                    raftStatus.installSnapshot = true;
                }
                return super.takeSnapshot(si);
            }
        };
        m = new DefaultSnapshotManager(groupConfig, kv, () -> kv.takeSnapshot(new SnapshotInfo(raftStatus)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void test(boolean separateExecutor) throws Exception {
        String dataDir = TestDir.createTestDir(DefaultSnapshotManager.class.getSimpleName()).getAbsolutePath();
        createManager(separateExecutor, dataDir, false);
        doInFiber(new FiberFrame<>() {
            private long index = 1;
            private static final int LOOP = 10;

            @Override
            protected FrameCallResult doFinally() {
                kv.stop(new DtTime(1, TimeUnit.SECONDS));
                m.stopFiber();
                return super.doFinally();
            }

            @Override
            public FrameCallResult execute(Void input) {
                kv.start();
                return Fiber.call(m.init(), this::afterInit);
            }

            private FrameCallResult afterInit(Snapshot snapshot) throws Exception {
                assertNull(snapshot);
                m.startFiber();
                return beforePut(null);
            }

            private FrameCallResult beforePut(Void v) throws Exception {
                if (index > LOOP) {
                    return afterLoop();
                }
                KvReq req = new KvReq(1, ("key" + index).getBytes(), ("value" + index).getBytes());
                RaftReqData rd = RaftReqData.build(LogHeader.TYPE_NORMAL, DtKV.BIZ_TYPE_PUT, req);
                rd.index = index;
                rd.timestamp = groupConfig.ts.wallClockMillis;
                RaftInput i = RaftInput.create(rd, null, req,
                        new DtTime(1, TimeUnit.SECONDS), false, null);
                ((RaftTask) i).init(groupConfig.ts.nanoTime);
                FiberFuture<Object> f = kv.exec(i);
                index++;
                return f.await(this::afterPut);
            }

            private FrameCallResult afterPut(Object result) {
                assertEquals(KvCodes.SUCCESS, ((KvResult) result).getBizCode());
                raftStatus.setLastApplied(index - 1);
                if ((index - 1) % 2 == 0) {
                    FiberFuture<Long> f = m.saveSnapshot();
                    return f.await(this::afterSave);
                } else {
                    return Fiber.resume(null, this::beforePut);
                }
            }

            private FrameCallResult afterSave(Long idx) {
                assertEquals(index - 1, idx);
                return Fiber.resume(null, this::beforePut);
            }

            private FrameCallResult afterLoop() throws Exception {
                kv.stop(new DtTime(1, TimeUnit.SECONDS));
                m.stopFiber();

                // make sure the delete snapshot file task done, otherwise the next init will fail
                CountDownLatch latch = new CountDownLatch(10);
                for (int i = 0; i < 10; i++) {
                    groupConfig.blockIoExecutor.submit(latch::countDown);
                }
                assertTrue(latch.await(3, TimeUnit.SECONDS));

                createManager(separateExecutor, dataDir, false);
                kv.start();
                return Fiber.call(m.init(), this::afterInit2);
            }

            private FrameCallResult afterInit2(Snapshot snapshot) {
                assertNotNull(snapshot);
                assertEquals(1, snapshot.getSnapshotInfo().lastIncludedTerm);
                assertEquals(LOOP, snapshot.getSnapshotInfo().lastIncludedIndex);

                m.startFiber();

                FiberFrame<Void> f = m.recover(snapshot);
                return Fiber.call(f, this::afterRecover);
            }

            private FrameCallResult afterRecover(Void v) {
                for (index = 1; index <= LOOP; index++) {
                    ByteArray key = new ByteArray(("key" + index).getBytes());
                    KvResult r = kv.get(key);
                    assertEquals(KvCodes.SUCCESS, r.getBizCode());
                    assertEquals("value" + index, new String(r.getNode().data));
                }

                File dir = new File(dataDir);
                dir = new File(dir, DefaultSnapshotManager.SNAPSHOT_DIR);
                File[] files = dir.listFiles();
                assertEquals(groupConfig.maxKeepSnapshots * 2, files == null ? 0 : files.length);

                return Fiber.frameReturn();
            }

        });
    }

    @Test
    void testCancel() throws Exception {
        String dataDir = TestDir.createTestDir(DefaultSnapshotManager.class.getSimpleName()).getAbsolutePath();
        createManager(false, dataDir, true);
        AtomicBoolean saveFinished = new AtomicBoolean();
        doInFiber(new FiberFrame<>() {
            @Override
            protected FrameCallResult doFinally() {
                kv.stop(new DtTime(1, TimeUnit.SECONDS));
                m.stopFiber();
                return super.doFinally();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                assertTrue(ex.getMessage().contains("cancel"));
                return Fiber.frameReturn();
            }

            @Override
            public FrameCallResult execute(Void input) {
                kv.start();
                m.startFiber();
                FiberFuture<Long> f = m.saveSnapshot();
                return f.await(this::afterSave);
            }

            private FrameCallResult afterSave(Long aLong) {
                saveFinished.set(true);
                return Fiber.frameReturn();
            }
        });
        assertFalse(saveFinished.get());
    }

    @Test
    void testStopFiberCompletesPendingSaveRequests() throws Exception {
        String dataDir = TestDir.createTestDir(DefaultSnapshotManager.class.getSimpleName()).getAbsolutePath();
        createManager(false, dataDir, false);
        doInFiber(() -> {
            // save loop fiber is not started, so the request stays in saveRequest
            FiberFuture<Long> f1 = m.saveSnapshot();
            m.stopFiber();
            assertTrue(f1.isDone());
            assertNotNull(f1.getEx());

            // requests after stopFiber complete exceptionally immediately
            FiberFuture<Long> f2 = m.saveSnapshot();
            assertTrue(f2.isDone());
            assertNotNull(f2.getEx());
        });
    }

    @Test
    void testReservedIndex() throws Exception {
        String dataDir = TestDir.createTestDir(DefaultSnapshotManager.class.getSimpleName()).getAbsolutePath();
        createManager(false, dataDir, false);
        doInFiber(new FiberFrame<>() {
            private int saveCount;

            @Override
            protected FrameCallResult doFinally() {
                kv.stop(new DtTime(1, TimeUnit.SECONDS));
                m.stopFiber();
                return super.doFinally();
            }

            @Override
            public FrameCallResult execute(Void input) {
                kv.start();
                return Fiber.call(m.init(), this::afterInit);
            }

            private FrameCallResult afterInit(Snapshot snapshot) {
                assertNull(snapshot);
                m.startFiber();
                return step(null);
            }

            private FrameCallResult step(Void v) {
                // save at index 2, 4, 6, 8
                if (saveCount == 4) {
                    return Fiber.frameReturn();
                }
                raftStatus.setLastApplied((saveCount + 1) * 2L);
                saveCount++;
                return m.saveSnapshot().await(this::afterSave);
            }

            private FrameCallResult afterSave(Long idx) {
                return Fiber.resume(null, this::step);
            }
        });
        // with keep=2 the list is [6, 8], so reserved is the second newest
        WaitUtil.waitUtil(() -> raftStatus.reservedSnapshotIndex == 6);
    }

    @Test
    void testKeep0DeleteByFirstValidIndex() throws Exception {
        String dataDir = TestDir.createTestDir(DefaultSnapshotManager.class.getSimpleName()).getAbsolutePath();
        createManager(false, dataDir, false);
        groupConfig.maxKeepSnapshots = 0;
        doInFiber(new FiberFrame<>() {
            private int saveCount;

            @Override
            protected FrameCallResult doFinally() {
                kv.stop(new DtTime(1, TimeUnit.SECONDS));
                m.stopFiber();
                return super.doFinally();
            }

            @Override
            public FrameCallResult execute(Void input) {
                kv.start();
                return Fiber.call(m.init(), this::afterInit);
            }

            private FrameCallResult afterInit(Snapshot snapshot) {
                assertNull(snapshot);
                m.startFiber();
                return step(null);
            }

            private FrameCallResult step(Void v) {
                if (saveCount == 3) {
                    raftStatus.firstValidIndex = 100;
                }
                // save at index 2, 4, 6, 8; the last save deletes 2/4/6 (all < firstValidIndex)
                if (saveCount == 4) {
                    return Fiber.frameReturn();
                }
                raftStatus.setLastApplied((saveCount + 1) * 2L);
                saveCount++;
                return m.saveSnapshot().await(this::afterSave);
            }

            private FrameCallResult afterSave(Long idx) {
                return Fiber.resume(null, this::step);
            }
        });
        WaitUtil.waitUtil(() -> raftStatus.reservedSnapshotIndex == 8);
        WaitUtil.waitUtil(() -> m.savedSnapshots.size() == 1);
        assertEquals(8, m.savedSnapshots.getFirst().lastIncludeIndex);
    }

    @Test
    void testInstallRestartClearsSnapshots() throws Exception {
        String dataDir = TestDir.createTestDir(DefaultSnapshotManager.class.getSimpleName()).getAbsolutePath();
        createManager(false, dataDir, false);
        doInFiber(new FiberFrame<>() {
            @Override
            protected FrameCallResult doFinally() {
                kv.stop(new DtTime(1, TimeUnit.SECONDS));
                m.stopFiber();
                return super.doFinally();
            }

            @Override
            public FrameCallResult execute(Void input) {
                kv.start();
                return Fiber.call(m.init(), this::afterInit);
            }

            private FrameCallResult afterInit(Snapshot snapshot) {
                assertNull(snapshot);
                m.startFiber();
                raftStatus.setLastApplied(2);
                return m.saveSnapshot().await(v -> {
                    raftStatus.setLastApplied(4);
                    return m.saveSnapshot().await(this::afterSecondSave);
                });
            }

            private FrameCallResult afterSecondSave(Long idx) {
                return Fiber.frameReturn();
            }
        });
        WaitUtil.waitUtil(() -> raftStatus.reservedSnapshotIndex == 2);
        File snapshotDir = new File(dataDir, DefaultSnapshotManager.SNAPSHOT_DIR);
        WaitUtil.waitUtil(() -> fileCount(snapshotDir) == 4);

        // restart in install-snapshot state: init deletes all snapshot files
        createManager(false, dataDir, false);
        raftStatus.installSnapshot = true;
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(m.init(), this::afterInit);
            }

            private FrameCallResult afterInit(Snapshot snapshot) {
                assertNull(snapshot);
                return Fiber.frameReturn();
            }
        });
        WaitUtil.waitUtil(() -> fileCount(snapshotDir) == 0);
    }

    private static int fileCount(File dir) {
        File[] files = dir.listFiles();
        return files == null ? -1 : files.length;
    }

    @Test
    void testOpenSnapshotForInstall() throws Exception {
        String dataDir = TestDir.createTestDir(DefaultSnapshotManager.class.getSimpleName()).getAbsolutePath();
        createManager(false, dataDir, false);
        groupConfig.maxKeepSnapshots = 0;
        groupConfig.installOldestSnapshot = true;
        // small block size to force multi-block snapshot files
        groupConfig.diskSnapshotBufferSize = 64;
        raftStatus.setRole(RaftRole.leader);
        doInFiber(new FiberFrame<>() {
            private int index = 1;
            private DefaultSnapshotManager.FileSnapshotInfo fsi;
            private final ByteBuffer readBuf = ByteBuffer.allocate(64);
            private final ByteArrayOutputStream out = new ByteArrayOutputStream();

            @Override
            protected FrameCallResult doFinally() {
                kv.stop(new DtTime(1, TimeUnit.SECONDS));
                m.stopFiber();
                return super.doFinally();
            }

            @Override
            public FrameCallResult execute(Void input) {
                kv.start();
                return Fiber.call(m.init(), this::afterInit);
            }

            private FrameCallResult afterInit(Snapshot s) {
                m.startFiber();
                return step(null);
            }

            private FrameCallResult step(Void v) {
                // save at index 2, 4, 6
                if (index > 6) {
                    raftStatus.firstValidIndex = 3;
                    return Fiber.call(m.openSnapshotForInstall(), this::afterOpen);
                }
                KvReq req = new KvReq(1, ("key" + index).getBytes(),
                        ("value" + index + "0".repeat(40)).getBytes());
                RaftReqData rd = RaftReqData.build(LogHeader.TYPE_NORMAL, DtKV.BIZ_TYPE_PUT, req);
                rd.index = index;
                rd.timestamp = groupConfig.ts.wallClockMillis;
                RaftInput i = RaftInput.create(rd, null, req,
                        new DtTime(1, TimeUnit.SECONDS), false, null);
                ((RaftTask) i).init(groupConfig.ts.nanoTime);
                index++;
                return kv.exec(i).await(this::afterPut);
            }

            private FrameCallResult afterPut(Object result) {
                assertEquals(KvCodes.SUCCESS, ((KvResult) result).getBizCode());
                raftStatus.setLastApplied(index - 1);
                if ((index - 1) % 2 == 0) {
                    return m.saveSnapshot().await(this::afterSave);
                }
                return Fiber.resume(null, this::step);
            }

            private FrameCallResult afterSave(Long idx) {
                return Fiber.resume(null, this::step);
            }

            private FrameCallResult afterOpen(Snapshot s) {
                // the oldest one with lastIncludedIndex >= firstValidIndex
                assertEquals(4, s.getSnapshotInfo().lastIncludedIndex);
                FileSnapshot snapshot = assertInstanceOf(FileSnapshot.class, s);
                for (DefaultSnapshotManager.FileSnapshotInfo x : m.savedSnapshots) {
                    if (x.lastIncludeIndex == 4) {
                        fsi = x;
                    }
                }
                assertNotNull(fsi);
                assertTrue(fsi.openReads.contains(snapshot));
                return readAll(s);
            }

            private FrameCallResult readAll(Snapshot s) {
                readBuf.clear();
                return s.readNext(readBuf).await(bytes -> afterReadChunk(s, bytes));
            }

            private FrameCallResult afterReadChunk(Snapshot s, Integer bytes) throws IOException {
                if (bytes > 0) {
                    byte[] chunk = new byte[bytes];
                    readBuf.get(chunk);
                    out.write(chunk);
                    return readAll(s);
                }
                assertArrayEquals(deframe(Files.readAllBytes(fsi.dataFile.toPath()), fsi.bufferSize),
                        out.toByteArray());
                s.close();
                assertTrue(fsi.openReads.isEmpty());

                // none qualifies, fallback to take latest
                raftStatus.firstValidIndex = 100;
                return Fiber.call(m.openSnapshotForInstall(), this::afterOpenFallback);
            }

            private FrameCallResult afterOpenFallback(Snapshot s) {
                assertFalse(s instanceof FileSnapshot);
                assertEquals(6, s.getSnapshotInfo().lastIncludedIndex);
                s.close();

                raftStatus.firstValidIndex = 0;
                raftStatus.installSnapshot = true;
                return Fiber.call(m.openSnapshotForInstall(), this::afterOpenInInstallState);
            }

            private FrameCallResult afterOpenInInstallState(Snapshot s) {
                assertNull(s);
                raftStatus.installSnapshot = false;
                raftStatus.setRole(RaftRole.follower);
                return Fiber.call(m.openSnapshotForInstall(), this::afterOpenNotLeader);
            }

            private FrameCallResult afterOpenNotLeader(Snapshot s) {
                assertNull(s);
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    void testCorruptedSnapshotSkipped() throws Exception {
        String dataDir = TestDir.createTestDir(DefaultSnapshotManager.class.getSimpleName()).getAbsolutePath();
        createManager(false, dataDir, false);
        groupConfig.maxKeepSnapshots = 0;
        groupConfig.installOldestSnapshot = true;
        raftStatus.setRole(RaftRole.leader);
        doInFiber(new FiberFrame<>() {
            private int saveCount;
            private DefaultSnapshotManager.FileSnapshotInfo corruptFsi;
            private Snapshot snapshot;

            @Override
            protected FrameCallResult doFinally() {
                kv.stop(new DtTime(1, TimeUnit.SECONDS));
                m.stopFiber();
                return super.doFinally();
            }

            @Override
            public FrameCallResult execute(Void input) {
                kv.start();
                return Fiber.call(m.init(), this::afterInit);
            }

            private FrameCallResult afterInit(Snapshot s) throws IOException {
                m.startFiber();
                return step(null);
            }

            private FrameCallResult step(Void v) throws IOException {
                // save at index 2, 4, 6
                if (saveCount == 3) {
                    for (DefaultSnapshotManager.FileSnapshotInfo x : m.savedSnapshots) {
                        if (x.lastIncludeIndex == 2) {
                            corruptFsi = x;
                        }
                    }
                    assertNotNull(corruptFsi);
                    try (RandomAccessFile raf = new RandomAccessFile(corruptFsi.dataFile, "rw")) {
                        int size = raf.readInt();
                        raf.seek(4 + size / 2);
                        int b = raf.read();
                        raf.seek(4 + size / 2);
                        raf.write(b ^ 0xFF);
                    }
                    raftStatus.firstValidIndex = 0;
                    return Fiber.call(m.openSnapshotForInstall(), this::afterOpen);
                }
                raftStatus.setLastApplied((saveCount + 1) * 2L);
                saveCount++;
                return m.saveSnapshot().await(this::afterSave);
            }

            private FrameCallResult afterSave(Long idx) {
                return Fiber.resume(null, this::step);
            }

            private FrameCallResult afterOpen(Snapshot s) {
                assertEquals(2, s.getSnapshotInfo().lastIncludedIndex);
                snapshot = s;
                return s.readNext(ByteBuffer.allocate(1024 * 1024)).await(this::afterRead);
            }

            private FrameCallResult afterRead(Integer bytes) {
                throw new AssertionError("read should fail with crc error");
            }

            @Override
            protected FrameCallResult handle(Throwable ex) throws Throwable {
                if (ex instanceof AssertionError) {
                    throw ex;
                }
                assertTrue(ex.getMessage() != null && ex.getMessage().contains("crc"));
                assertTrue(corruptFsi.bad);
                snapshot.close();
                return Fiber.frameReturn();
            }
        });

        // the corrupted snapshot is marked bad, next open selects the following one
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(m.openSnapshotForInstall(), this::afterOpen);
            }

            private FrameCallResult afterOpen(Snapshot s) {
                assertEquals(4, s.getSnapshotInfo().lastIncludedIndex);
                s.close();
                return Fiber.frameReturn();
            }
        });
    }

    private static byte[] deframe(byte[] file, int stride) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        int pos = 0;
        while (pos < file.length) {
            int size = ByteBuffer.wrap(file, pos, 4).getInt();
            o.write(file, pos + 4, size);
            pos += stride;
        }
        return o.toByteArray();
    }

    @Test
    void testDeleteWaitsInFlightRead() throws Exception {
        String dataDir = TestDir.createTestDir(DefaultSnapshotManager.class.getSimpleName()).getAbsolutePath();
        createManager(false, dataDir, false);
        groupConfig.maxKeepSnapshots = 0;
        groupConfig.installOldestSnapshot = true;
        raftStatus.setRole(RaftRole.leader);
        ExecutorService singleIo = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "test-single-io");
            t.setDaemon(true);
            return t;
        });
        groupConfig.blockIoExecutor = singleIo;
        CountDownLatch ioBlocker = new CountDownLatch(1);
        CompletableFuture<Void> done = new CompletableFuture<>();
        DefaultSnapshotManager.FileSnapshotInfo[] readerHolder = new DefaultSnapshotManager.FileSnapshotInfo[1];
        File snapshotDir = new File(dataDir, DefaultSnapshotManager.SNAPSHOT_DIR);
        try {
            fiberGroup.fireFiber("test-delete-waits", new FiberFrame<>() {
                private int saveCount;
                private FileSnapshot r1;
                private FileSnapshot r2;
                private Snapshot w1;
                private Snapshot w2;
                private FiberFuture<Integer> readFuture;

                @Override
                protected FrameCallResult handle(Throwable ex) {
                    done.completeExceptionally(ex);
                    return Fiber.frameReturn();
                }

                @Override
                public FrameCallResult execute(Void input) {
                    kv.start();
                    return Fiber.call(m.init(), this::afterInit);
                }

                private FrameCallResult afterInit(Snapshot s) {
                    m.startFiber();
                    return step(null);
                }

                private FrameCallResult step(Void v) {
                    // save at index 2, 4, 8
                    if (saveCount == 3) {
                        raftStatus.firstValidIndex = 3;
                        return Fiber.call(m.openSnapshotForInstall(), this::afterOpen1);
                    }
                    raftStatus.setLastApplied(saveCount == 2 ? 8 : (saveCount + 1) * 2L);
                    saveCount++;
                    return m.saveSnapshot().await(this::afterSave);
                }

                private FrameCallResult afterSave(Long idx) {
                    return Fiber.resume(null, this::step);
                }

                private FrameCallResult afterOpen1(Snapshot s) {
                    w1 = s;
                    r1 = (FileSnapshot) s;
                    readerHolder[0] = r1.fsi;
                    return Fiber.call(m.openSnapshotForInstall(), this::afterOpen2);
                }

                private FrameCallResult afterOpen2(Snapshot s) {
                    w2 = s;
                    r2 = (FileSnapshot) s;
                    assertEquals(r1.getSnapshotInfo().lastIncludedIndex, r2.getSnapshotInfo().lastIncludedIndex);
                    // block the single io thread so the next read stays in-flight
                    singleIo.submit(() -> {
                        try {
                            ioBlocker.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    readFuture = r1.readNext(ByteBuffer.allocateDirect(1024 * 1024));
                    raftStatus.firstValidIndex = 5;
                    return Fiber.call(m.deleteOldFiles(), this::afterDelete);
                }

                private FrameCallResult afterDelete(Void v) {
                    // snapshots at 2 and 4 are deleted, 8 is kept
                    assertEquals(1, m.savedSnapshots.size());
                    assertEquals(8, m.savedSnapshots.getFirst().lastIncludeIndex);
                    // the in-flight read completes normally
                    assertTrue(readFuture.isDone());
                    assertNull(readFuture.getEx());
                    assertTrue(readFuture.getResult() > 0);
                    assertEquals(0, readerHolder[0].busy);
                    // both readers are closed by the deletion path
                    FiberFuture<Integer> f1 = r1.readNext(ByteBuffer.allocateDirect(1024));
                    FiberFuture<Integer> f2 = r2.readNext(ByteBuffer.allocateDirect(1024));
                    assertTrue(f1.isDone() && f1.getEx() != null);
                    assertTrue(f2.isDone() && f2.getEx() != null);
                    w1.close();
                    w2.close();
                    return Fiber.frameReturn();
                }

                @Override
                protected FrameCallResult doFinally() {
                    done.complete(null);
                    kv.stop(new DtTime(1, TimeUnit.SECONDS));
                    m.stopFiber();
                    return Fiber.frameReturn();
                }
            });
            WaitUtil.waitUtil(() -> readerHolder[0] != null && readerHolder[0].closing);
            ioBlocker.countDown();
            done.get(Tick.tick(5000), TimeUnit.MILLISECONDS);
            WaitUtil.waitUtil(() -> fileCount(snapshotDir) == 2);
        } finally {
            ioBlocker.countDown();
            singleIo.shutdownNow();
        }
    }
}
