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
package com.github.dtprj.dongting.dtmq.server;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.sm.DefaultSnapshotManager;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.test.TestDir;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
class DtMQTest extends BaseFiberTest {

    private File dataDir;
    private RaftGroupConfigEx config;
    private RaftStatusImpl raftStatus;
    private DtMQ mq;
    private DefaultSnapshotManager snapshotManager;

    private void setup() {
        dataDir = TestDir.createTestDir(DtMQTest.class.getSimpleName());
        RaftGroupConfigEx c = new RaftGroupConfigEx(1, "1", "");
        c.fiberGroup = fiberGroup;
        c.ts = dispatcher.ts;
        c.dataDir = dataDir.getAbsolutePath();
        c.blockIoExecutor = MockExecutors.ioExecutor();
        c.mqIdxItemsPerFile = 256;
        c.mqIdxCacheBlocks = 64;
        c.mqIdxFlushThreshold = 64;
        c.mqIdxFlushBatchItems = 128;
        c.mqIdxFlushIntervalMillis = 60_000;
        config = c;
        resetRaftStatusAndBiz();
    }

    private void resetRaftStatusAndBiz() {
        raftStatus = new RaftStatusImpl(1, dispatcher.ts);
        raftStatus.nodeIdOfMembers = Set.of(1);
        raftStatus.nodeIdOfObservers = Set.of();
        raftStatus.nodeIdOfPreparedMembers = Set.of();
        raftStatus.nodeIdOfPreparedObservers = Set.of();
        raftStatus.lastAppliedTerm = 1;
        config.raftStatus = raftStatus;
        mq = new DtMQ(config);
        snapshotManager = new DefaultSnapshotManager(config, mq,
                () -> mq.takeSnapshot(new SnapshotInfo(raftStatus)), idx -> {
        });
    }

    private FiberFuture<Object> exec(long queueId, long raftIndex) {
        RaftReqData rd = RaftReqData.build(LogHeader.TYPE_NORMAL, 0);
        rd.bizKey = queueId;
        rd.index = raftIndex;
        rd.timestamp = queueId * 1_000_000 + raftIndex * 100;
        RaftTask rt = (RaftTask) RaftInput.create(rd, null, null, null, false, null);
        rt.raftLogPosition = queueId * 1_000_000 + raftIndex * 10;
        return mq.exec(rt);
    }

    private abstract class BaseFrame extends FiberFrame<Void> {
        @Override
        protected FrameCallResult doFinally() {
            mq.stop(new DtTime(1, TimeUnit.SECONDS));
            snapshotManager.stopFiber();
            return Fiber.frameReturn();
        }
    }

    @Test
    void testSaveSnapshotAndRecover() throws Exception {
        setup();
        doInFiber(new BaseFrame() {
            private long index;

            @Override
            public FrameCallResult execute(Void input) {
                mq.start();
                return Fiber.call(snapshotManager.init(), this::afterInit);
            }

            private FrameCallResult afterInit(Snapshot s) {
                assertNull(s);
                snapshotManager.startFiber();
                return Fiber.resume(null, this::nextPut);
            }

            private FrameCallResult nextPut(Void v) {
                index++;
                if (index > 130) {
                    return afterPuts();
                }
                long queueId = index <= 125 ? 1 : 2;
                FiberFuture<Object> f = exec(queueId, index);
                return f.await(this::afterPut);
            }

            private FrameCallResult afterPut(Object result) {
                assertNull(result);
                long queueId = index <= 125 ? 1 : 2;
                long expectNextSeq = queueId == 1 ? index : index - 125;
                assertEquals(expectNextSeq, mq.manager.get(queueId).nextSeq);
                raftStatus.setLastApplied(index);
                return Fiber.resume(null, this::nextPut);
            }

            private FrameCallResult afterPuts() {
                FiberFuture<Long> f = snapshotManager.saveSnapshot();
                return f.await(this::afterSave);
            }

            private FrameCallResult afterSave(Long idx) {
                assertEquals(130, idx);
                return Fiber.frameReturn();
            }
        });

        File queue1Dir = new File(new File(dataDir, "mqIdx"), "1");
        String[] files = queue1Dir.list();
        assertTrue(files != null && files.length > 0);

        doInFiber(new BaseFrame() {
            private FiberFuture<Object> f1;
            private FiberFuture<Object> f2;

            @Override
            public FrameCallResult execute(Void input) {
                resetRaftStatusAndBiz();
                mq.start();
                return Fiber.call(snapshotManager.init(), this::afterInit);
            }

            private FrameCallResult afterInit(Snapshot s) {
                assertNotNull(s);
                assertEquals(130, s.getSnapshotInfo().lastIncludedIndex);
                return Fiber.call(snapshotManager.recover(s), this::afterRecover);
            }

            private FrameCallResult afterRecover(Void v) {
                QueueIdxInfo q1 = mq.manager.get(1);
                QueueIdxInfo q2 = mq.manager.get(2);
                assertEquals(125, q1.nextSeq);
                assertEquals(5, q2.nextSeq);
                assertTrue(q1.needLoadHead);
                assertTrue(q2.needLoadHead);
                assertEquals(-1, mq.manager.getIdxItemInCache(1, 124));
                f1 = exec(1, 131);
                f2 = exec(1, 132);
                FiberFuture<Object> f3 = exec(1, 133);
                return f3.await(this::afterExec);
            }

            private FrameCallResult afterExec(Object result) {
                assertNull(result);
                assertNull(f1.getEx());
                assertNull(f2.getEx());
                assertEquals(128, mq.manager.get(1).nextSeq);
                for (long seq = 120; seq < 125; seq++) {
                    long pos = mq.manager.getIdxItemInCache(1, seq);
                    assertEquals(1_000_000 + (seq + 1) * 10, pos, "seq " + seq);
                }
                FiberFuture<Object> f = exec(2, 132);
                return f.await(this::afterExec2);
            }

            private FrameCallResult afterExec2(Object result) {
                assertNull(result);
                assertEquals(6, mq.manager.get(2).nextSeq);
                for (long seq = 0; seq < 5; seq++) {
                    long pos = mq.manager.getIdxItemInCache(2, seq);
                    assertEquals(2_000_000 + (seq + 126) * 10, pos, "seq " + seq);
                }
                FiberFuture<Object> f = exec(1, 134);
                return f.await(this::afterExec3);
            }

            private FrameCallResult afterExec3(Object result) {
                assertNull(result);
                assertEquals(129, mq.manager.get(1).nextSeq);
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    void testInstallSnapshot() throws Exception {
        setup();
        doInFiber(new BaseFrame() {
            private long index;

            @Override
            public FrameCallResult execute(Void input) {
                mq.start();
                snapshotManager.startFiber();
                return Fiber.resume(null, this::nextPut);
            }

            private FrameCallResult nextPut(Void v) {
                index++;
                if (index > 10) {
                    return afterPuts();
                }
                FiberFuture<Object> f = exec(1, index);
                return f.await(this::afterPut);
            }

            private FrameCallResult afterPut(Object result) {
                assertNull(result);
                assertEquals(index, mq.manager.get(1).nextSeq);
                raftStatus.setLastApplied(index);
                return Fiber.resume(null, this::nextPut);
            }

            private FrameCallResult afterPuts() {
                return mq.manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                File queue1Dir = new File(new File(dataDir, "mqIdx"), "1");
                String[] files = queue1Dir.list();
                assertTrue(files != null && files.length > 0);
                return mq.startInstall(true).await(this::afterClean);
            }

            private FrameCallResult afterClean(Void v) {
                File mqIdxDir = new File(dataDir, "mqIdx");
                String[] left = mqIdxDir.list();
                assertEquals(0, left == null ? 0 : left.length);
                ByteBuffer buf = ByteBuffer.allocate(2 * MqSnapshot.ITEM_BYTES);
                buf.putLong(7).putLong(300);
                buf.putLong(8).putLong(256);
                buf.flip();
                FiberFuture<Void> f = mq.installSnapshot(100, 1, 0, false, buf);
                return f.await(this::afterChunk);
            }

            private FrameCallResult afterChunk(Void v) {
                FiberFuture<Void> f = mq.installSnapshot(100, 1, 2 * MqSnapshot.ITEM_BYTES,
                        true, ByteBuffer.allocate(0));
                return f.await(this::afterDone);
            }

            private FrameCallResult afterDone(Void v) {
                QueueIdxInfo q7 = mq.manager.get(7);
                QueueIdxInfo q8 = mq.manager.get(8);
                assertEquals(300, q7.nextSeq);
                assertEquals(256, q8.nextSeq);
                assertTrue(q7.needLoadHead);
                assertEquals(-1L, mq.manager.getIdxItemInCache(7, 299));
                assertEquals(-1L, mq.manager.getIdxItemInCache(7, 255));

                FiberFuture<Object> f = exec(7, 101);
                return f.await(this::afterExec);
            }

            private FrameCallResult afterExec(Object result) {
                assertNull(result);
                assertEquals(301, mq.manager.get(7).nextSeq);
                assertEquals(0L, mq.manager.getIdxItemInCache(7, 299));
                return mq.manager.flusher.flushAll().await(this::afterPostInstallFlush);
            }

            private FrameCallResult afterPostInstallFlush(Void v) throws Exception {
                File dir7 = new File(new File(dataDir, "mqIdx"), "7");
                byte[] data = Files.readAllBytes(new File(dir7, "00000000000000008192").toPath());
                ByteBuffer buf = ByteBuffer.wrap(data);
                CRC32C crc = new CRC32C();
                for (long seq = 256; seq < 300; seq++) {
                    int off = (int) (seq - 256) * MqIdxManager.ITEM_LEN;
                    assertEquals(0L, buf.getLong(off), "pos of seq " + seq);
                    assertEquals(0L, buf.getLong(off + 8), "timestamp of seq " + seq);
                    assertEquals(0, buf.getInt(off + 24), "size of seq " + seq);
                    crc.reset();
                    crc.update(data, off, MqIdxManager.ITEM_LEN - 4);
                    assertEquals((int) crc.getValue(), buf.getInt(off + 28), "crc of seq " + seq);
                }
                int off = (300 - 256) * MqIdxManager.ITEM_LEN;
                assertEquals(7_001_010L, buf.getLong(off));
                assertEquals(7_010_100L, buf.getLong(off + 8));
                assertEquals(LogHeader.ITEM_HEADER_SIZE, buf.getInt(off + 24));
                assertFalse(new File(new File(dataDir, "mqIdx"), "8").exists());
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    void testInstallRetryAfterAbort() throws Exception {
        setup();
        doInFiber(new BaseFrame() {
            @Override
            public FrameCallResult execute(Void input) {
                mq.start();
                return mq.startInstall(true).await(this::afterFirstClean);
            }

            private FrameCallResult afterFirstClean(Void v) {
                ByteBuffer buf = ByteBuffer.allocate(MqSnapshot.ITEM_BYTES);
                buf.putLong(7).putLong(300).flip();
                FiberFuture<Void> f = mq.installSnapshot(100, 1, 0, false, buf);
                return f.await(this::afterAbort);
            }

            private FrameCallResult afterAbort(Void v) {
                return mq.startInstall(true).await(this::afterSecondClean);
            }

            private FrameCallResult afterSecondClean(Void v) {
                ByteBuffer buf = ByteBuffer.allocate(MqSnapshot.ITEM_BYTES);
                buf.putLong(9).putLong(512).flip();
                FiberFuture<Void> f = mq.installSnapshot(101, 1, 0, false, buf);
                return f.await(this::afterRetryChunk);
            }

            private FrameCallResult afterRetryChunk(Void v) {
                FiberFuture<Void> f = mq.installSnapshot(101, 1, MqSnapshot.ITEM_BYTES,
                        true, ByteBuffer.allocate(0));
                return f.await(this::afterDone);
            }

            private FrameCallResult afterDone(Void v) {
                assertNull(mq.manager.get(7));
                QueueIdxInfo q9 = mq.manager.get(9);
                assertNotNull(q9);
                assertEquals(512, q9.nextSeq);
                return Fiber.frameReturn();
            }
        });
    }
}
