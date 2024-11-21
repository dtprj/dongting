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
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.impl.InitFiberFrame;
import com.github.dtprj.dongting.raft.impl.RaftCancelException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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

    @BeforeEach
    void setup() throws Exception {
        dataDir = TestDir.testDir(DefaultRaftLogTest.class.getSimpleName());
        init();
    }

    private void init() throws Exception {
        raftStatus = new RaftStatusImpl(dispatcher.getTs());
        RaftServerConfig serverConfig = new RaftServerConfig();
        config = new RaftGroupConfigEx(1, "1", "1");
        config.setFiberGroup(fiberGroup);
        config.setDataDir(dataDir);
        config.setBlockIoExecutor(MockExecutors.ioExecutor());
        config.setTs(raftStatus.getTs());
        config.setRaftStatus(raftStatus);
        raftStatus.setTailCache(new TailCache(config, raftStatus));
        statusManager = new StatusManager(config);
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(statusManager.initStatusFile(), this::resume);
            }

            private FrameCallResult resume(Void v) {
                return Fiber.frameReturn();
            }
        });

        raftLog = new DefaultRaftLog(config, statusManager, null, 1);
        raftLog.idxItemsPerFile = 8;
        config.setIdxCacheSize(4);
        config.setIdxFlushThreshold(2);
        raftLog.logFileSize = 1024;
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
        ArrayList<LogItem> list = new ArrayList<>();
        for (int i = 0; i < totalSizes.length; i++) {
            LogItem li = createItem(config, 100, 100, index++, totalSizes[i], bizHeaderLen[i]);
            list.add(li);
        }
        append(list);
    }

    private void append(List<LogItem> list) throws Exception {
        long lastIdx = list.get(list.size() - 1).getIndex();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                raftLog.append(list);
                return waitWriteFinish(null);
            }

            private FrameCallResult waitWriteFinish(Void v) {
                if (raftStatus.getLastForceLogIndex() < lastIdx) {
                    return raftStatus.getLogForceFinishCondition().await(1000, this::waitWriteFinish);
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
        int[] totalSizes = new int[]{400, 400, 512, 200, 400};
        int[] bizHeaderLen = new int[]{1, 0, 400, 100, 1};
        append(1, totalSizes, bizHeaderLen);
        raftStatus.setCommitIndex(5);
        raftStatus.setLastApplied(5);
        raftStatus.setLastLogIndex(5);
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
        raftStatus.setCommitIndex(5);
        raftStatus.setLastApplied(5);
        raftStatus.setLastLogIndex(5);
        raftStatus.setLastForceLogIndex(5);

        // test delete
        File dir = new File(new File(dataDir), "log");

        {
            Supplier<Boolean> deleted = fileDeleted(dir, 0);
            doInFiber(() -> raftLog.markTruncateByIndex(3, 1000));
            Thread.sleep(2);
            assertFalse(deleted.get());
            plus1Hour();
            TestUtil.waitUtil(deleted);
        }
        {
            plus1Hour();
            doInFiber(() -> raftLog.markTruncateByTimestamp(raftStatus.getTs().getWallClockMillis(), 0));

            // can't delete after next persist index and apply index, so only delete to index 4
            Supplier<Boolean> deleted = fileDeleted(dir, 1024);
            plus1Hour();
            TestUtil.waitUtil(deleted);
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
        doInFiber(() -> TestUtil.plus1Hour(raftStatus.getTs()));
    }

    @Test
    void testTruncate() throws Exception {
        int[] totalSizes = new int[]{800, 512, 256, 256, 512};
        int[] bizHeaderLen = new int[]{10, 10, 10, 10, 10};
        append(1, totalSizes, bizHeaderLen);
        raftStatus.setCommitIndex(1);
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

        testLoader(() -> raftLog.openIterator(() -> false));
        testLoader(() -> new FileLogLoader(raftLog.idxFiles, raftLog.logFiles, config,
                null, () -> false, 99));
        doInFiber(new FiberFrame<>() {
            final RaftLog.LogIterator it = raftLog.openIterator(() -> true);

            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(it.next(1, 1, 500000), this::afterNext);
            }

            private FrameCallResult afterNext(List<LogItem> logItems) {
                Assertions.fail();
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                assertInstanceOf(RaftCancelException.class, ex);
                return Fiber.frameReturn();
            }
        });
        doInFiber(new FiberFrame<>() {
            int count;
            final RaftLog.LogIterator it = raftLog.openIterator(() -> count++ >= 1);

            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(it.next(1, 1, 500000), this::afterNext);
            }

            private FrameCallResult afterNext(List<LogItem> logItems) {
                Assertions.fail();
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                assertInstanceOf(RaftCancelException.class, ex);
                return Fiber.frameReturn();
            }
        });

        RaftInput input = new RaftInput(0, null, null, null, false);
        raftStatus.getTailCache().put(3, new RaftTask(raftStatus.getTs(), 0, input, null));
        doInFiber(new FiberFrame<>() {
            final RaftLog.LogIterator it = raftLog.openIterator(() -> false);

            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(it.next(1, 1000, 500000), this::afterNext);
            }

            private FrameCallResult afterNext(List<LogItem> logItems) {
                assertEquals(2, logItems.size());
                return Fiber.frameReturn();
            }
        });
    }

    private void testLoader(Supplier<RaftLog.LogIterator> creator) throws Exception {
        final int total = 12;
        doInFiber(new FiberFrame<>() {
            final RaftLog.LogIterator it = creator.get();

            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(it.next(1, total, 500000), this::afterNext);
            }

            private FrameCallResult afterNext(List<LogItem> logItems) {
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

            private FrameCallResult afterNext(List<LogItem> logItems) throws Exception {
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

            private FrameCallResult afterNext(List<LogItem> logItems) throws Exception {
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
        ArrayList<LogItem> list = new ArrayList<>();
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
        raftStatus.setLastLogIndex(list.get(list.size() - 1).getIndex());

        for (int i = 0; i <= list.size(); i++) {
            TailCache tailCache = raftStatus.getTailCache();
            tailCache.cleanAll();
            for (int j = list.size() - i; j < list.size(); j++) {
                RaftInput ri = new RaftInput(0, null, null, null, false);
                RaftTask t = new RaftTask(raftStatus.getTs(), 0, ri, null);
                LogItem li = list.get(j);
                t.setItem(li);
                tailCache.put(li.getIndex(), t);
            }
            testMatch();
        }
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
