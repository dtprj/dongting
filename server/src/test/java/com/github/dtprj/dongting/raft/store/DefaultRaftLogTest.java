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

import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.buf.TwoLevelPool;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
@SuppressWarnings("SameParameterValue")
public class DefaultRaftLogTest {
    private String dataDir;
    private RaftStatusImpl raftStatus;
    private RaftGroupConfig config;
    private StatusManager statusManager;
    private DefaultRaftLog raftLog;
    private boolean mockPersistSyncFail;

    @BeforeEach
    public void setup() {
        dataDir = TestDir.testDir(DefaultRaftLogTest.class.getSimpleName());
        mockPersistSyncFail = false;
    }

    private void init() throws Exception {
        raftStatus = new RaftStatusImpl();
        config = new RaftGroupConfig(1, "1", "1");
        config.setDataDir(dataDir);
        config.setRaftExecutor(MockExecutors.raftExecutor());
        config.setIoExecutor(MockExecutors.ioExecutor());
        config.setStopIndicator(() -> false);
        config.setTs(raftStatus.getTs());
        config.setDirectPool(TwoLevelPool.getDefaultFactory().apply(config.getTs(), true));
        config.setHeapPool(new RefBufferFactory(TwoLevelPool.getDefaultFactory().apply(config.getTs(), false), 0));
        // TODO just fix compile
        statusManager = new StatusManager(config, raftStatus) {
            @Override
            public void persistSync() {
                super.persistSync();
                if (mockPersistSyncFail) {
                    mockPersistSyncFail = false;
                    throw new SecurityException("mock fail");
                }
            }
        };
        statusManager.initStatusFile();
        config.setRaftStatus(raftStatus);
        config.setIoExecutor(MockExecutors.ioExecutor());

        raftLog = new DefaultRaftLog(config, statusManager);
        raftLog.idxItemsPerFile = 8;
        raftLog.idxMaxCacheItems = 4;
        raftLog.logFileSize = 1024;
        raftLog.logWriteBufferSize = 512;
        // TODO just fix compile
        raftLog.init(null);
    }

    @AfterEach
    public void tearDown() {
        raftLog.close();
        statusManager.close();
    }

    @Test
    public void generalTest() throws Exception {
        init();
        int[] totalSizes = new int[]{400, 400, 512};
        int[] bizHeaderLen = new int[]{1, 0, 400};
        LogItem[] items = FileLogLoaderTest.createItems(config, 1, totalSizes, bizHeaderLen);
        // TODO just fix compile
        raftLog.append();

        raftStatus.setLastLogIndex(3);
        int term = items[0].getTerm();
        CompletableFuture<Pair<Integer, Long>> f = raftLog.tryFindMatchPos(term, 2, () -> false);
        assertEquals(new Pair<>(term, 2L), f.get());

        RaftLog.LogIterator it = raftLog.openIterator(() -> false);
        assertEquals(3, it.next(1, 3, 1000000).get().size());
        it.close();

        tearDown();

        init();
        raftStatus.setCommitIndex(3);
        raftStatus.setLastApplied(3);
        raftStatus.setLastLogIndex(3);
        items = FileLogLoaderTest.createItems(config, 4, totalSizes, bizHeaderLen);
        // TODO just fix compile
        raftLog.append();
        assertEquals(5, raftLog.logFiles.getLogFile(2048).firstIndex);
        // replace
        items = FileLogLoaderTest.createItems(config, 4, new int[]{200}, new int[]{100});
        // TODO just fix compile
        raftLog.append();
        assertEquals(0, raftLog.logFiles.getLogFile(2048).firstIndex);

        it = raftLog.openIterator(() -> false);
        assertEquals(4, it.next(1, 4, 1000000).get().size());
        it.close();

        tearDown();

        // test recover
        init();
    }

    @Test
    public void testDelete() throws Exception {
        init();
        int[] totalSizes = new int[]{400, 400, 512, 200, 400};
        int[] bizHeaderLen = new int[]{1, 0, 400, 100, 1};
        LogItem[] items = FileLogLoaderTest.createItems(config, 1, totalSizes, bizHeaderLen);
        // TODO just fix compile
        raftLog.append();
        raftStatus.setCommitIndex(5);
        raftStatus.setLastApplied(5);
        raftStatus.setLastLogIndex(5);
        items = FileLogLoaderTest.createItems(config, 6, totalSizes, bizHeaderLen);
        // TODO just fix compile
        raftLog.append();

        // test delete
        File dir = new File(new File(dataDir), "log");

        {
            Supplier<Boolean> deleted = fileDeleted(dir, 0);
            CompletableFuture.runAsync(() -> raftLog.markTruncateByIndex(3, 0),
                    MockExecutors.raftExecutor()).join();
            TestUtil.plus1Hour(raftStatus.getTs());
            assertFalse(deleted.get());
            CompletableFuture.runAsync(() -> raftLog.doDelete(), MockExecutors.raftExecutor()).join();
            TestUtil.waitUtil(deleted);
        }

        {
            TestUtil.plus1Hour(raftStatus.getTs());
            CompletableFuture.runAsync(() -> raftLog.markTruncateByTimestamp(
                    raftStatus.getTs().getWallClockMillis(), 0), MockExecutors.raftExecutor()).join();
            // can't delete after next persist index and apply index, so only delete to index 4
            Supplier<Boolean> deleted = fileDeleted(dir, 1024);
            assertFalse(deleted.get());
            TestUtil.plus1Hour(raftStatus.getTs());
            TestUtil.waitUtilInExecutor(MockExecutors.raftExecutor(), () -> {
                // may in deleting status, need retry
                raftLog.doDelete();
                return deleted.get();
            });
        }
    }

    private static Supplier<Boolean> fileDeleted(File dir, long startIndex) {
        return () -> {
            String[] names = dir.list();
            String firstFileName = String.format("%020d", startIndex);
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

    @Test
    public void testTruncateFail() throws Exception {
        init();
        int[] totalSizes = new int[]{400, 400, 512, 200};
        int[] bizHeaderLen = new int[]{1, 0, 400, 100, 1};
        LogItem[] items = FileLogLoaderTest.createItems(config, 1, totalSizes, bizHeaderLen);
        // TODO just fix compile
        raftLog.append();

        LogItem[] items2 = FileLogLoaderTest.createItems(config, 3 , new int[]{200, 200}, new int[]{100, 100});
        mockPersistSyncFail = true;
        try {
            // TODO just fix compile
            // raftLog.append(Arrays.asList(items2)).get();
            fail();
        } catch (Exception e) {
            assertEquals(SecurityException.class, DtUtil.rootCause(e).getClass());
        }

        tearDown();

        init();
        assertEquals(1, raftLog.logFiles.getLogFile(0).firstIndex);
        assertEquals(0, raftLog.logFiles.getLogFile(1024).firstIndex);
    }
}
