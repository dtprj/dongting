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

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.impl.InitFiberFrame;
import com.github.dtprj.dongting.raft.impl.RaftCancelException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.raft.test.TestUtil;
import com.github.dtprj.dongting.test.TestDir;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static com.github.dtprj.dongting.raft.store.LogFileQueueTest.createItem;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class DefaultRaftLogTest extends BaseFiberTest {
    private String dataDir;
    private RaftStatusImpl raftStatus;
    private RaftGroupConfigEx config;
    private StatusManager statusManager;
    private DefaultRaftLog raftLog;
    private boolean lagStatusFile;

    @BeforeEach
    void setup() throws Exception {
        dataDir = TestDir.testDir(DefaultRaftLogTest.class.getSimpleName());
        init();
    }

    private void init() throws Exception {
        raftStatus = new RaftStatusImpl(1, dispatcher.ts);
        RaftServerConfig serverConfig = new RaftServerConfig();
        config = new RaftGroupConfigEx(1, "1", "1");
        config.fiberGroup = fiberGroup;
        config.dataDir = dataDir;
        config.blockIoExecutor = MockExecutors.ioExecutor();
        config.ts = raftStatus.ts;
        config.raftStatus = raftStatus;
        raftStatus.tailCache = new TailCache(config, raftStatus);
        statusManager = lagStatusFile ? new LaggingStatusManager(config) : new StatusManager(config);
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(statusManager.initStatusFile(), this::resume);
            }

            private FrameCallResult resume(Void v) {
                return Fiber.frameReturn();
            }
        });

        config.idxItemsPerFile = 8;
        config.idxCacheSize = 4;
        config.idxFlushThreshold = 2;
        // shorten the periodic idx flush interval so tests don't wait for it
        config.idxFlushIntervalMillis = 20;
        config.logFileSize = 1024;
        raftLog = new DefaultRaftLog(config, statusManager, null, 1);
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                InitFiberFrame.initRaftStatus(raftStatus, fiberGroup, serverConfig);
                return Fiber.call(raftLog.init(), this::resume);
            }

            private FrameCallResult resume(Pair<Integer, Long> integerLongPair) {
                return Fiber.frameReturn();
            }
        });
    }

    @AfterEach
    void tearDown() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                FiberFuture<Void> f1 = raftLog.close();
                FiberFuture<Void> f2 = statusManager.close();
                return FiberFuture.allOf("close", f1, f2).await(this::justReturn);
            }
        });
    }

    private void append(long index, int[] totalSizes, int[] bizHeaderLen) throws Exception {
        ArrayList<RaftTask> list = new ArrayList<>();
        for (int i = 0; i < totalSizes.length; i++) {
            RaftTask li = createItem(config, 100, 100, index++, totalSizes[i], bizHeaderLen[i]);
            list.add(li);
        }
        append(list);
    }

    private void append(List<RaftTask> list) throws Exception {
        long lastIdx = list.get(list.size() - 1).reqData.index;
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(raftLog.append(list), v -> waitWriteFinish(null));
            }

            private FrameCallResult waitWriteFinish(Void v) {
                if (raftStatus.lastForceLogIndex < lastIdx) {
                    return raftStatus.logForceFinishCondition.await(1000, this::waitWriteFinish);
                } else {
                    return Fiber.frameReturn();
                }
            }
        });
    }

    @Test
    void testInit() throws Exception {
        int[] totalSizes = new int[]{400, 400, 512};
        int[] bizHeaderLen = new int[]{1, 0, 400};
        append(1, totalSizes, bizHeaderLen);
        tearDown();
        init();
    }

    @Test
    void testDelete() throws Exception {
        try {
            prepareDeleteEnv();

            // test delete
            File dir = new File(new File(dataDir), "log");

            {
                Supplier<Boolean> deleted = fileDeleted(dir, 0);
                doInFiber(() -> raftLog.markTruncateByIndex(3, 1000));
                Thread.sleep(2);
                assertFalse(deleted.get());
                plus1Hour();
                WaitUtil.waitUtil(deleted);
            }
            {
                plus1Hour();
                doInFiber(() -> raftLog.markTruncateByTimestamp(raftStatus.ts.wallClockMillis, 0));

                // can't delete after next persist index and apply index, so only delete to index 4
                Supplier<Boolean> deleted = fileDeleted(dir, 1024);
                plus1Hour();
                WaitUtil.waitUtil(deleted);
            }
        } finally {
            doInFiber(()->{
                // Restore timestamp to current time because this test uses plus1Hour() to modify
                // dispatcher.ts multiple times. Must restore to current time to avoid subsequent
                // tests seeing "nanoTime go back" error when Dispatcher thread calls ts.refresh()
                TestUtil.updateTimestamp(raftStatus.ts, System.nanoTime(), System.currentTimeMillis());
                BugLog.reset();
            });
        }
    }

    private static Supplier<Boolean> fileDeleted(File dir, long startPos) {
        return () -> {
            String[] names = dir.list();
            String firstFileName = String.format("%020d", startPos);
            if (names != null) {
                for (String n : names) {
                    if (n.equals(firstFileName)) {
                        return false;
                    }
                }
            }
            return true;
        };
    }

    private void plus1Hour() throws Exception {
        doInFiber(() -> TestUtil.plus1Hour(raftStatus.ts));
    }

    private void prepareDeleteEnv() throws Exception {
        int[] totalSizes = new int[]{400, 400, 512, 200, 400};
        int[] bizHeaderLen = new int[]{1, 0, 400, 100, 1};
        append(1, totalSizes, bizHeaderLen);
        raftStatus.commitIndex = 5;
        raftStatus.setLastApplied(5);
        raftStatus.lastLogIndex = 5;
        append(6, totalSizes, bizHeaderLen);

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                // to fire idx flush
                return raftLog.close().await(this::resume);
            }

            private FrameCallResult resume(Void unused) {
                return statusManager.close().await(this::justReturn);
            }
        });

        init();
        raftStatus.commitIndex = 5;
        raftStatus.setLastApplied(5);
        raftStatus.lastLogIndex = 5;
        raftStatus.lastForceLogIndex = 5;
        raftStatus.reservedSnapshotIndex = 10;
    }

    @Test
    void testFirstValidRecoverStaleStatus() throws Exception {
        try {
            prepareDeleteEnv();
            File dir = new File(new File(dataDir), "log");
            doInFiber(() -> raftLog.markTruncateByIndex(5, 0));
            WaitUtil.waitUtil(() -> fileDeleted(dir, 1024).get()
                    && raftStatus.firstValidIndex == 5
                    && raftStatus.firstValidPos == 2048);

            // simulate deletion succeeded but the persist was lost:
            // rewrite stale values (1/0) to the status file
            tearDown();
            init();
            doInFiber(() -> {
                raftStatus.firstValidIndex = 1;
                raftStatus.firstValidPos = 0;
                statusManager.persistAsync();
            });
            tearDown();

            // recovery must fix the stale values from what the disks actually hold
            init();
            doInFiber(() -> {
                assertEquals(5, raftStatus.firstValidIndex);
                assertEquals(2048, raftStatus.firstValidPos);
            });
        } finally {
            doInFiber(() -> {
                TestUtil.updateTimestamp(raftStatus.ts, System.nanoTime(), System.currentTimeMillis());
                BugLog.reset();
            });
        }
    }

    // simulates that the status file update is not finished, by dropping persist requests
    private static class LaggingStatusManager extends StatusManager {
        volatile boolean dropPersistRequests;

        LaggingStatusManager(RaftGroupConfigEx c) {
            super(c);
        }

        @Override
        public void persistAsync() {
            if (!dropPersistRequests) {
                super.persistAsync();
            }
        }
    }

    @Test
    void testDeleteGuardByStatusFilePersistIndex() throws Exception {
        try {
            int[] totalSizes = new int[]{400, 400, 512, 200, 400};
            int[] bizHeaderLen = new int[]{1, 0, 400, 100, 1};
            append(1, totalSizes, bizHeaderLen);
            raftStatus.commitIndex = 5;
            raftStatus.setLastApplied(5);
            raftStatus.lastLogIndex = 5;
            append(6, totalSizes, bizHeaderLen);
            // let the close below persist idx to 10 and save KEY_PERSIST_IDX_INDEX=10 to status file
            raftStatus.commitIndex = 10;

            doInFiber(new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    return raftLog.close().await(this::resume);
                }

                private FrameCallResult resume(Void unused) {
                    return statusManager.close().await(this::justReturn);
                }
            });

            // restart with the lagging status manager
            lagStatusFile = true;
            init();
            LaggingStatusManager lsm = (LaggingStatusManager) statusManager;
            // the status file is "not updated" from now on, so lastPersistedIdxIndex keeps
            // the restored value 10 even if new values are submitted by idx force
            lsm.dropPersistRequests = true;

            // the durable value is restored from the status file
            doInFiber(() -> assertEquals(10, statusManager.lastPersistedIdxIndex));

            append(11, totalSizes, bizHeaderLen);
            append(16, totalSizes, bizHeaderLen);
            raftStatus.commitIndex = 20;
            raftStatus.setLastApplied(20);
            raftStatus.lastLogIndex = 20;
            raftStatus.lastForceLogIndex = 20;
            raftStatus.reservedSnapshotIndex = 25;

            // idx data of entry 15 is forced and submitted to the status file, but the update
            // is "dropped", so the durable value is still 10
            WaitUtil.waitUtil(() -> raftLog.idxFiles.submittedPersistIndexInStatusFile >= 15, 10_000);
            doInFiber(() -> assertEquals(10, statusManager.lastPersistedIdxIndex));

            File dir = new File(new File(dataDir), "log");
            // the mark bound is min(lastApplied=20, lastPersistedIdxIndex=10, reservedSnapshotIndex=25,
            // index=15) = 10, so only files whose next file starts at index <= 10 are marked
            doInFiber(() -> raftLog.markTruncateByIndex(15, 0));

            // files before 3072 are deleted since the durable value 10 covers their next file's firstIndex
            WaitUtil.waitUtil(fileDeleted(dir, 3072));
            // the next file of 4096 starts at index 11 > 10, so the file at 4096 is not even marked
            assertFalse(fileDeleted(dir, 4096).get());

            // after the status file is updated again, the durable value catches up. update it
            // directly instead of waiting for the periodic idx force to trigger the update
            lsm.dropPersistRequests = false;
            doInFiber(() -> statusManager.persistAsync());
            WaitUtil.waitUtil(() -> statusManager.lastPersistedIdxIndex >= 15, 10_000);

            // re-mark with bound=15 and deletion continues
            doInFiber(() -> raftLog.markTruncateByIndex(15, 0));
            WaitUtil.waitUtil(fileDeleted(dir, 5120));
            // the file at 6144 is not marked since its next file starts at index 17 > 15, so it is kept
            assertFalse(fileDeleted(dir, 6144).get());
        } finally {
            lagStatusFile = false;
            if (statusManager instanceof LaggingStatusManager) {
                ((LaggingStatusManager) statusManager).dropPersistRequests = false;
            }
        }
    }

    @Test
    void testTruncate() throws Exception {
        int[] totalSizes = new int[]{800, 512, 256, 256, 512};
        int[] bizHeaderLen = new int[]{10, 10, 10, 10, 10};
        append(1, totalSizes, bizHeaderLen);
        raftStatus.commitIndex = 1;
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                LogFile logFile = raftLog.logFiles.getLogFile(2048);
                assertEquals(5, logFile.firstIndex);
                return Fiber.call(raftLog.truncateTail(5), this::resume1);
            }

            private FrameCallResult resume1(Void unused) {
                LogFile logFile = raftLog.logFiles.getLogFile(2048);
                assertEquals(0, logFile.firstIndex);
                logFile = raftLog.logFiles.getLogFile(1024);
                assertEquals(2, logFile.firstIndex);
                assertEquals(5, raftLog.logFiles.logAppender.nextPersistIndex);
                assertEquals(2048, raftLog.logFiles.logAppender.nextPersistPos);
                return Fiber.call(raftLog.truncateTail(3), this::resume2);
            }

            private FrameCallResult resume2(Void unused) {
                LogFile logFile = raftLog.logFiles.getLogFile(1024);
                assertEquals(2, logFile.firstIndex);
                assertEquals(3, raftLog.logFiles.logAppender.nextPersistIndex);
                assertEquals(1536, raftLog.logFiles.logAppender.nextPersistPos);
                return Fiber.call(raftLog.truncateTail(2), this::resume3);
            }

            private FrameCallResult resume3(Void unused) {
                LogFile logFile = raftLog.logFiles.getLogFile(1024);
                assertEquals(0, logFile.firstIndex);
                assertEquals(2, raftLog.logFiles.logAppender.nextPersistIndex);
                assertEquals(800, raftLog.logFiles.logAppender.nextPersistPos);
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    void testFileLogLoader() throws Exception {
        // file 1
        // 1: 256 bytes, no header
        // 2: 256 bytes, no body
        // 3: LogHeader.ITEM_HEADER_SIZE bytes, no header, no body
        // 4: 512 - LogHeader.ITEM_HEADER_SIZE bytes, fill rest file
        int[] totalSizes = new int[]{256, 256, LogHeader.ITEM_HEADER_SIZE, 512 - LogHeader.ITEM_HEADER_SIZE};
        int[] bizHeaderLen = new int[]{0, 256 - LogHeader.ITEM_HEADER_SIZE - 4, 0, 100};
        append(1, totalSizes, bizHeaderLen);
        // file 2, started from 5, with (LogHeader.ITEM_HEADER_SIZE - 1) bytes not used
        append(5, new int[]{150, 200, 250, 1024 - 600 - (LogHeader.ITEM_HEADER_SIZE - 1)}, new int[]{10, 20, 150, 100});
        // file 3, started from 9, with end magic item
        append(9, new int[]{600}, new int[]{300});
        // file 4, started from 10, with end magic item just fill the file
        append(10, new int[]{1024 - LogHeader.ITEM_HEADER_SIZE}, new int[]{300});
        // file 5, started from 11, total 12 items
        append(11, new int[]{100, 100}, new int[]{10, 10});

        testLoader(() -> raftLog.openIterator(() -> false, true));
        testLoader(() -> new FileLogLoader(raftLog.idxFiles, raftLog.logFiles, config,
                null, () -> false, true, 99));
        // test cancel indicator
        doInFiber(new FiberFrame<>() {
            final RaftLog.LogIterator it = raftLog.openIterator(() -> true, true);

            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(it.next(1, 1, 500000), this::afterNext);
            }

            private FrameCallResult afterNext(List<RaftTask> logItems) {
                fail();
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                assertInstanceOf(RaftCancelException.class, ex);
                return Fiber.frameReturn();
            }
        });
        // test cancel indicator
        doInFiber(new FiberFrame<>() {
            int count;
            final RaftLog.LogIterator it = raftLog.openIterator(() -> count++ >= 1, true);

            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(it.next(1, 1, 500000), this::afterNext);
            }

            private FrameCallResult afterNext(List<RaftTask> logItems) {
                fail();
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                assertInstanceOf(RaftCancelException.class, ex);
                return Fiber.frameReturn();
            }
        });
        RaftReqData rd = RaftReqData.build(LogHeader.TYPE_NORMAL, 0);
        RaftTask input = new RaftTask(rd, null, null,
                null, false, null);
        raftStatus.tailCache.put(3, input);
        // test cancel if tail cache has next item
        doInFiber(new FiberFrame<>() {
            final RaftLog.LogIterator it = raftLog.openIterator(() -> false, true);

            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(it.next(1, 1000, 500000), this::afterNext);
            }

            private FrameCallResult afterNext(List<RaftTask> logItems) {
                assertEquals(2, logItems.size());
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    void testFileLogLoaderStreamDecode() throws Exception {
        // TYPE_NORMAL items are decoded through the codec factory on the fly while reading.
        // the last item has biz header but no body (bodyLen == 0).
        int[] totalSizes = new int[]{200, 300, 150, 400, LogHeader.ITEM_HEADER_SIZE + 50, 220,
                LogHeader.computeTotalLen(20, 0)};
        int[] bizHeaderLen = new int[]{30, 0, 60, 100, 0, 10, 20};
        ArrayList<RaftTask> list = new ArrayList<>();
        long index = 1;
        for (int i = 0; i < totalSizes.length; i++) {
            list.add(createItem(config, LogHeader.TYPE_NORMAL, 100, 100, index++, totalSizes[i], bizHeaderLen[i]));
        }
        append(list);

        RaftCodecFactory codecFactory = new RaftCodecFactory() {
            @Override
            public DecoderCallback<ByteArray> createHeaderCallback(int bizType, DecodeContext context) {
                return new ByteArray.Callback();
            }

            @Override
            public DecoderCallback<ByteArray> createBodyCallback(int bizType, DecodeContext context) {
                return new ByteArray.Callback();
            }
        };
        final int total = totalSizes.length;
        doInFiber(new FiberFrame<>() {
            // small read buffer forces biz header/body to span multiple reads, i.e. stream decode
            final RaftLog.LogIterator it = new FileLogLoader(raftLog.idxFiles, raftLog.logFiles, config,
                    codecFactory, () -> false, true, 64);

            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(it.next(1, total, 500000), this::afterNext);
            }

            private FrameCallResult afterNext(List<RaftTask> logItems) throws Exception {
                assertEquals(total, logItems.size());
                for (RaftTask rt : logItems) {
                    // decode == true does not build the raw buffer
                    assertNull(rt.reqData.buffer);
                    assertEquals(LogHeader.computeTotalLen(rt.reqData.bizHeaderLen, rt.reqData.bodyLen),
                            rt.reqData.totalLen);
                    assertStreamDecoded(rt.bizHeader, rt.reqData.bizHeaderLen);
                    assertStreamDecoded(rt.bizBody, rt.reqData.bodyLen);
                    rt.reqData.release();
                }
                it.close();
                return Fiber.frameReturn();
            }
        });
    }

    private void assertStreamDecoded(Object decoded, int expectLen) {
        if (expectLen == 0) {
            assertNull(decoded);
            return;
        }
        byte[] data = ((ByteArray) decoded).getData();
        assertEquals(expectLen, data.length);
        for (int i = 0; i < expectLen; i++) {
            assertEquals((byte) i, data[i], "content mismatch at " + i);
        }
    }

    private void testLoader(Supplier<RaftLog.LogIterator> creator) throws Exception {
        final int total = 12;
        doInFiber(new FiberFrame<>() {
            final RaftLog.LogIterator it = creator.get();

            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(it.next(1, total, 500000), this::afterNext);
            }

            private FrameCallResult afterNext(List<RaftTask> logItems) {
                assertEquals(total, logItems.size());
                return Fiber.call(it.next(total + 1, 1, 500000), this::afterNext);
            }

            @Override
            protected FrameCallResult handle(Throwable ex) throws Exception {
                assertInstanceOf(ChecksumException.class, ex);
                it.close();
                return Fiber.frameReturn();
            }
        });
        doInFiber(new FiberFrame<>() {
            final RaftLog.LogIterator it = creator.get();
            int index = 1;

            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(it.next(index, 2, 500000), this::afterNext);
            }

            private FrameCallResult afterNext(List<RaftTask> logItems) throws Exception {
                assertEquals(2, logItems.size());
                index += 2;
                if (index <= total) {
                    return Fiber.resume(null, this);
                } else {
                    it.close();
                    return Fiber.frameReturn();
                }
            }
        });
        doInFiber(new FiberFrame<>() {
            final RaftLog.LogIterator it = creator.get();
            int index = 1;

            @Override
            public FrameCallResult execute(Void input) {
                int limit = total - index + 1;
                return Fiber.call(it.next(index, limit, 300), this::afterNext);
            }

            private FrameCallResult afterNext(List<RaftTask> logItems) throws Exception {
                index += logItems.size();
                if (index <= total) {
                    return Fiber.resume(null, this);
                } else {
                    it.close();
                    return Fiber.frameReturn();
                }
            }

            @Override
            protected FrameCallResult handle(Throwable ex) throws Throwable {
                return super.handle(ex);
            }
        });
    }

    @Test
    void testTryFindMatchPos() throws Exception {
        ArrayList<RaftTask> list = new ArrayList<>();
        // file 1
        list.add(createItem(config, 1, 0, 1, 256, 50));
        list.add(createItem(config, 1, 1, 2, 256, 50));
        list.add(createItem(config, 1, 1, 3, 256, 50));
        list.add(createItem(config, 1, 1, 4, 256, 50));
        // file 2
        list.add(createItem(config, 2, 1, 5, 256, 50));// change term
        list.add(createItem(config, 2, 2, 6, 256, 50));
        list.add(createItem(config, 2, 2, 7, 256, 50));
        list.add(createItem(config, 4, 2, 8, 256, 50));// change term
        // file 3
        list.add(createItem(config, 4, 4, 9, 256, 50));
        list.add(createItem(config, 4, 4, 10, 256, 50));
        list.add(createItem(config, 5, 4, 11, 256, 50));// change term
        list.add(createItem(config, 5, 5, 12, 256, 50));
        append(list);
        raftStatus.lastLogIndex = list.get(list.size() - 1).reqData.index;

        TailCache tailCache = raftStatus.tailCache;
        for (int i = 0; i <= list.size(); i++) {
            tailCache.cleanAll();
            for (int j = list.size() - i; j < list.size(); j++) {
                RaftReqData rd = RaftReqData.build(LogHeader.TYPE_NORMAL, 0);
                RaftTask li = list.get(j);
                rd.term = li.reqData.term;
                rd.prevLogTerm = li.reqData.prevLogTerm;
                rd.index = li.reqData.index;
                rd.timestamp = li.reqData.timestamp;
                LogHeader.writeAndComputeCrc(rd, new java.util.zip.CRC32C(), rd.buffer.getBuffer(), 0);
                RaftTask t = new RaftTask(rd, null, null, false);
                tailCache.put(li.reqData.index, t);
            }
            testMatch();
        }

        tailCache.cleanAll();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                FiberFrame<Pair<Integer, Long>> f = raftLog.tryFindMatchPos(1, 1, () -> true);
                return Fiber.call(f, this::afterFind);
            }

            private FrameCallResult afterFind(Pair<Integer, Long> r) {
                fail();
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                assertInstanceOf(RaftCancelException.class, ex);
                return Fiber.frameReturn();
            }
        });

        // mark delete
        raftLog.logFiles.getLogFile(0).deleteTimestamp = 100;
        testMatch(2, 5, 2, 5);
        testMatch(1, 4, -1, -1);
    }

    private void testMatch() throws Exception {
        testMatch(1, 1, 1, 1);
        testMatch(2, 1, -1, -1);
        testMatch(2, 2, 1, 1);

        testMatch(1, 2, 1, 2);

        testMatch(1, 4, 1, 4);
        testMatch(1, 5, 1, 4);
        testMatch(3, 5, 1, 4);

        testMatch(2, 5, 2, 5);
        testMatch(3, 6, 2, 5);

        testMatch(2, 7, 2, 7);
        testMatch(2, 8, 2, 7);
        testMatch(3, 8, 2, 7);
        testMatch(5, 8, 2, 7);

        testMatch(4, 8, 4, 8);
        testMatch(5, 9, 4, 8);

        testMatch(4, 9, 4, 9);
        testMatch(5, 10, 4, 9);

        testMatch(5, 11, 5, 11);
        testMatch(6, 12, 5, 11);

        testMatch(5, 12, 5, 12);
        testMatch(5, 13, 5, 12);
        testMatch(6, 13, 5, 12);
    }

    private void testMatch(int suggestTerm, long suggestIndex, int expectTerm, long expectIndex) throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                FiberFrame<Pair<Integer, Long>> f = raftLog.tryFindMatchPos(suggestTerm, suggestIndex, () -> false);
                return Fiber.call(f, this::afterFind);
            }

            private FrameCallResult afterFind(Pair<Integer, Long> r) {
                if (r == null) {
                    //noinspection MisorderedAssertEqualsArguments
                    assertEquals(expectTerm, -1);
                    //noinspection MisorderedAssertEqualsArguments
                    assertEquals(expectIndex, -1);
                } else {
                    assertEquals(expectTerm, r.getLeft());
                    assertEquals(expectIndex, r.getRight());
                }
                return Fiber.frameReturn();
            }
        });
    }
}
