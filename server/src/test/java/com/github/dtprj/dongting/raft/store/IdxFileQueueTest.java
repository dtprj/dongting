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

import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author huangli
 */
public class IdxFileQueueTest extends BaseFiberTest {

    private IdxFileQueue idxFileQueue;
    private RaftStatusImpl raftStatus;
    private StatusManager statusManager;
    private File dir;

    @BeforeEach
    public void setup() throws Exception {
        dir = TestDir.createTestDir(IdxFileQueueTest.class.getSimpleName());
        idxFileQueue = createFileQueue();
    }

    private IdxFileQueue createFileQueue() throws Exception {
        return createFileQueue(true);
    }

    private IdxFileQueue createFileQueue(boolean init) throws Exception {

        RaftGroupConfigEx c = new RaftGroupConfigEx(1, "1", "1");
        c.setIdxCacheSize(4);
        c.setIdxFlushThreshold(2);
        c.setBlockIoExecutor(MockExecutors.ioExecutor());
        raftStatus = new RaftStatusImpl(dispatcher.getTs());
        raftStatus.setTailCache(new TailCache(c, raftStatus));
        c.setRaftStatus(raftStatus);
        c.setTs(raftStatus.getTs());
        c.setFiberGroup(fiberGroup);
        c.setDataDir(dir.getAbsolutePath());
        statusManager = new StatusManager(c);
        AtomicReference<IdxFileQueue> result = new AtomicReference<>();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(statusManager.initStatusFile(), this::afterStatusInit);
            }

            private FrameCallResult afterStatusInit(Void unused) throws Exception {
                IdxFileQueue q = new IdxFileQueue(dir, statusManager, c, 8);
                if(init) {
                    q.initQueue();
                    return Fiber.call(q.initRestorePos(), r -> afterInit(q));
                } else {
                    result.set(q);
                    return Fiber.frameReturn();
                }
            }

            private FrameCallResult afterInit(IdxFileQueue q) {
                q.startFibers();
                q.initialized = true;
                result.set(q);
                return Fiber.frameReturn();
            }
        });
        return result.get();
    }

    @AfterEach
    public void tearDown() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return idxFileQueue.close().await(this::afterFlushFinish);
            }

            private FrameCallResult afterFlushFinish(Void unused) {
                return statusManager.close().await(this::justReturn);
            }
        });
    }

    @Test
    public void testConstructor() {
        RaftGroupConfigEx c = new RaftGroupConfigEx(1, "1", "1");
        assertThrows(IllegalArgumentException.class, () -> new IdxFileQueue(
                null, null, c, 511));
    }

    @Test
    public void testPut1() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                for (int i = 1; i <= 10; i++) {
                    idxFileQueue.put(i, i * 100);
                }
                assertEquals(10, idxFileQueue.cache.size());
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testPut2() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                for (int i = 1; i <= 30; i++) {
                    raftStatus.setCommitIndex(i - 1);
                    raftStatus.setLastForceLogIndex(i - 1);
                    idxFileQueue.put(i, i * 100);
                }
                raftStatus.setCommitIndex(30);
                raftStatus.setLastForceLogIndex(30);
                return waitFlush(null);
            }

            private FrameCallResult waitFlush(Void unused) {
                if (idxFileQueue.needWaitFlush()) {
                    return Fiber.call(idxFileQueue.waitFlush(), this::waitFlush);
                } else {
                    if (idxFileQueue.getNextPersistIndex() < 29) {
                        return Fiber.yield(this::waitFlush);
                    }
                    return Fiber.frameReturn();
                }
            }
        });
    }

    @Test
    public void testPutError1() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                assertThrows(RaftException.class, () -> idxFileQueue.put(10, 1000));
                assertThrows(RaftException.class, () -> idxFileQueue.put(0, 1000));
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testPutError2() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                for (int i = 1; i <= 10; i++) {
                    raftStatus.setCommitIndex(i - 1);
                    raftStatus.setLastForceLogIndex(i - 1);
                    idxFileQueue.put(i, i * 100);
                }
                assertThrows(RaftException.class, () -> idxFileQueue.put(5, 500));
                return Fiber.frameReturn();
            }
        });
    }

    private class LoadLogPosFrame extends FiberFrame<Void> {
        private final long index;
        private final long expectResult;

        @Override
        protected FrameCallResult handle(Throwable ex) throws Throwable {
            if (expectResult >= 0) {
                throw ex;
            }
            return Fiber.frameReturn();
        }

        public LoadLogPosFrame(long index, long expectResult) {
            this.index = index;
            this.expectResult = expectResult;
        }

        @Override
        public FrameCallResult execute(Void input) {
            return Fiber.call(idxFileQueue.loadLogPos(index), this::resume);
        }

        private FrameCallResult resume(Long result) {
            if (result == null || result != expectResult) {
                throw new RuntimeException("result=" + result + ", expect=" + expectResult);
            }
            return Fiber.frameReturn();
        }
    }

    @Test
    public void testTruncate() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                for (int i = 1; i <= 10; i++) {
                    idxFileQueue.put(i, i * 100);
                }
                idxFileQueue.truncateTail(5);
                assertEquals(4, idxFileQueue.cache.size());

                raftStatus.setCommitIndex(3);
                raftStatus.setLastForceLogIndex(3);
                assertThrows(RaftException.class, () -> idxFileQueue.truncateTail(3));
                assertThrows(RaftException.class, () -> idxFileQueue.truncateTail(5));
                idxFileQueue.truncateTail(4);
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testSyncLoad() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                for (int i = 1; i <= 30; i++) {
                    raftStatus.setCommitIndex(i - 1);
                    raftStatus.setLastForceLogIndex(i - 1);
                    idxFileQueue.put(i, i * 100);
                }
                return idxFileQueue.close().await(this::afterIdxClose);
            }

            private FrameCallResult afterIdxClose(Void unused) {
                return statusManager.close().await(this::justReturn);
            }
        });
        idxFileQueue = createFileQueue();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Throwable {
                return Fiber.call(idxFileQueue.initRestorePos(), this::resume);
            }

            private FrameCallResult resume(Pair<Long, Long> longLongPair) {
                return checkPos(null);
            }

            long checkIndex = 1;

            private FrameCallResult checkPos(Void v) {
                if (checkIndex >= 30) {
                    // delete a file
                    return Fiber.call(idxFileQueue.deleteFirstFile(), this::afterDelete);
                }
                FiberFrame<Void> f = new LoadLogPosFrame(checkIndex, checkIndex * 100);
                checkIndex++;
                return Fiber.call(f, this::checkPos);
            }

            private FrameCallResult afterDelete(Void unused) {
                assertEquals(idxFileQueue.indexToPos(8), idxFileQueue.queueStartPosition);
                return Fiber.call(new LoadLogPosFrame(1, -1), this::afterCheck1);
            }

            private FrameCallResult afterCheck1(Void unused) {
                return Fiber.call(new LoadLogPosFrame(30, -1), this::justReturn);
            }
        });
    }

    @Test
    public void testInit1() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                for (int i = 1; i <= 30; i++) {
                    raftStatus.setCommitIndex(i - 1);
                    raftStatus.setLastForceLogIndex(i - 1);
                    idxFileQueue.put(i, i * 100);
                }
                return waitFlush(null);
            }

            private FrameCallResult waitFlush(Void v) {
                if (idxFileQueue.needWaitFlush()) {
                    return Fiber.call(idxFileQueue.waitFlush(), this::waitFlush);
                }
                return idxFileQueue.close().await(this::afterFlushFinish);
            }

            private FrameCallResult afterFlushFinish(Void unused) {
                return statusManager.close().await(this::justReturn);
            }
        });

        idxFileQueue = createFileQueue(false);
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Throwable {
                idxFileQueue.initQueue();
                return Fiber.call(idxFileQueue.initRestorePos(), this::resume);
            }

            private FrameCallResult resume(Pair<Long, Long> p) {
                assertEquals(29, p.getLeft());
                assertEquals(2900, p.getRight());
                assertEquals(30, idxFileQueue.getNextIndex());
                assertEquals(30, idxFileQueue.getNextPersistIndex());
                assertEquals(0, idxFileQueue.queueStartPosition);
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testInit2() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                for (int i = 1; i <= 30; i++) {
                    raftStatus.setCommitIndex(i - 1);
                    raftStatus.setLastForceLogIndex(i - 1);
                    idxFileQueue.put(i, i * 100);
                }
                return waitFlush(null);
            }

            private FrameCallResult waitFlush(Void v) {
                if (idxFileQueue.needWaitFlush()) {
                    return Fiber.call(idxFileQueue.waitFlush(), this::waitFlush);
                }
                // delete a file
                return Fiber.call(idxFileQueue.deleteFirstFile(), this::afterDelete);
            }

            private FrameCallResult afterDelete(Void unused) {
                assertEquals(idxFileQueue.indexToPos(8), idxFileQueue.queueStartPosition);
                return idxFileQueue.close().await(this::afterIdxClose);
            }

            private FrameCallResult afterIdxClose(Void unused) {
                statusManager.getProperties().put(IdxFileQueue.KEY_PERSIST_IDX_INDEX, "2");
                statusManager.persistAsync(false);
                return statusManager.waitUpdateFinish(this::afterUpdateStatus);
            }

            private FrameCallResult afterUpdateStatus(Void unused) {
                return statusManager.close().await(this::justReturn);
            }
        });

        idxFileQueue = createFileQueue(false);
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Throwable {
                idxFileQueue.initQueue();
                return Fiber.call(idxFileQueue.initRestorePos(), this::resume);
            }

            private FrameCallResult resume(Pair<Long, Long> p) {
                assertEquals(8, p.getLeft());
                assertEquals(800, p.getRight());
                assertEquals(9, idxFileQueue.getNextIndex());
                assertEquals(9, idxFileQueue.getNextPersistIndex());
                assertEquals(8 << 3, idxFileQueue.queueStartPosition);
                // mock recover
                for (int i = 9; i <= 30; i++) {
                    idxFileQueue.put(i, i * 100);
                }
                return Fiber.frameReturn();
            }
        });
    }

}
