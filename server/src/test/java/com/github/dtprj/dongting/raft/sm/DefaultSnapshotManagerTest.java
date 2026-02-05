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

import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
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
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.test.TestDir;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class DefaultSnapshotManagerTest extends BaseFiberTest {
    private DefaultSnapshotManager m;
    private DtKV kv;
    private RaftStatusImpl raftStatus;
    private RaftGroupConfigEx groupConfig;

    private void createManager(boolean separateExecutor, String dataDir, boolean mockInstall) {
        raftStatus = new RaftStatusImpl(0, dispatcher.ts);
        raftStatus.nodeIdOfMembers = Set.of(1);
        raftStatus.nodeIdOfObservers = Set.of();
        raftStatus.nodeIdOfPreparedMembers = Set.of();
        raftStatus.nodeIdOfPreparedObservers = Set.of();
        raftStatus.lastAppliedTerm = 1;
        groupConfig = new RaftGroupConfigEx(0, "1", "");
        groupConfig.fiberGroup = fiberGroup;
        groupConfig.raftStatus = raftStatus;
        groupConfig.ts = dispatcher.ts;
        groupConfig.dataDir = dataDir;
        groupConfig.blockIoExecutor = MockExecutors.ioExecutor();
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
        m = new DefaultSnapshotManager(groupConfig, kv, idx -> {
        });
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
                RaftInput i = new RaftInput(DtKV.BIZ_TYPE_PUT, null, req,
                        new DtTime(1, TimeUnit.SECONDS), false);
                Timestamp ts = groupConfig.ts;
                FiberFuture<Object> f = kv.exec(index++, ts.wallClockMillis, ts.nanoTime, i);
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

}
