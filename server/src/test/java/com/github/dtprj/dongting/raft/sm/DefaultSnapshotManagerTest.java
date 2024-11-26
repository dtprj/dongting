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
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.dtkv.server.DtKV;
import com.github.dtprj.dongting.dtkv.server.KvConfig;
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.store.TestDir;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Set;
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

    private void createManager(boolean separateExecutor, String dataDir, boolean mockInstall) {
        raftStatus = new RaftStatusImpl(dispatcher.getTs()) {
            private int count;

            @Override
            public boolean isInstallSnapshot() {
                return mockInstall && count++ >= 2;
            }
        };
        raftStatus.setNodeIdOfMembers(Set.of(1));
        raftStatus.setNodeIdOfObservers(Set.of());
        raftStatus.setNodeIdOfPreparedMembers(Set.of());
        raftStatus.setNodeIdOfPreparedObservers(Set.of());
        raftStatus.setLastAppliedTerm(1);
        RaftGroupConfigEx groupConfig = new RaftGroupConfigEx(0, "1", "");
        groupConfig.setFiberGroup(fiberGroup);
        groupConfig.setRaftStatus(raftStatus);
        groupConfig.setTs(dispatcher.getTs());
        groupConfig.setDataDir(dataDir);
        groupConfig.setBlockIoExecutor(MockExecutors.ioExecutor());
        KvConfig kvConfig = new KvConfig();
        kvConfig.setUseSeparateExecutor(separateExecutor);
        kvConfig.setInitMapCapacity(16);
        kv = new DtKV(groupConfig, kvConfig);
        m = new DefaultSnapshotManager(groupConfig, kv);
    }

    @Test
    void test() throws Exception {
        test(false);
        test(true);
    }

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
                m.startFiber();
                return Fiber.call(m.init(), this::afterInit);
            }

            private FrameCallResult afterInit(Snapshot snapshot) {
                assertNull(snapshot);
                return beforePut(null);
            }

            private FrameCallResult beforePut(Void v) {
                if (index > LOOP) {
                    return afterLoop();
                }
                ByteArray key = new ByteArray(("key" + index).getBytes());
                ByteArray value = new ByteArray(("value" + index).getBytes());
                RaftInput i = new RaftInput(DtKV.BIZ_TYPE_PUT, key, value,
                        new DtTime(1, TimeUnit.SECONDS), false);
                FiberFuture<Object> f = kv.exec(index++, i);
                return f.await(this::afterPut);
            }

            private FrameCallResult afterPut(Object result) {
                assertEquals(KvCodes.CODE_SUCCESS, ((KvResult) result).getBizCode());
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

            private FrameCallResult afterLoop() {
                kv.stop(new DtTime(1, TimeUnit.SECONDS));
                m.stopFiber();

                createManager(separateExecutor, dataDir, false);
                kv.start();
                m.startFiber();
                return Fiber.call(m.init(), this::afterInit2);
            }

            private FrameCallResult afterInit2(Snapshot snapshot) {
                assertNotNull(snapshot);
                FiberFrame<Pair<Integer, Long>> f = m.recover(snapshot);
                return Fiber.call(f, this::afterRecover);
            }

            private FrameCallResult afterRecover(Pair<Integer, Long> p) {
                assertEquals(1, p.getLeft());
                assertEquals(LOOP, p.getRight().longValue());
                for (index = 1; index <= LOOP; index++) {
                    ByteArray key = new ByteArray(("key" + index).getBytes());
                    KvResult r = kv.get(key);
                    assertEquals(KvCodes.CODE_SUCCESS, r.getBizCode());
                    assertEquals("value" + index, new String(r.getNode().getData()));
                }

                File dir = new File(dataDir);
                dir = new File(dir, DefaultSnapshotManager.SNAPSHOT_DIR);
                File[] files = dir.listFiles();
                assertEquals(DefaultSnapshotManager.KEEP * 2, files == null ? 0 : files.length);

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