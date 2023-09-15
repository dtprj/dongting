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
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
@SuppressWarnings("SameParameterValue")
public class DefaultRaftLogTest {
    private String dataDir;
    private RaftStatusImpl raftStatus;
    private RaftGroupConfigEx config;
    private StatusManager statusManager;
    private DefaultRaftLog raftLog;

    @BeforeEach
    public void setup() {
        dataDir = TestDir.testDir(DefaultRaftLogTest.class.getSimpleName());
    }

    private void init() throws Exception {
        raftStatus = new RaftStatusImpl();
        config = new RaftGroupConfigEx(1, "1", "1");
        config.setDataDir(dataDir);
        config.setRaftExecutor(MockExecutors.raftExecutor());
        config.setStopIndicator(() -> false);
        config.setTs(raftStatus.getTs());
        config.setDirectPool(TwoLevelPool.getDefaultFactory().apply(config.getTs(), true));
        config.setHeapPool(new RefBufferFactory(TwoLevelPool.getDefaultFactory().apply(config.getTs(), false), 0));
        statusManager = new StatusManager(MockExecutors.ioExecutor(), raftStatus);
        statusManager.initStatusFileChannel(config.getDataDir(), config.getStatusFile());
        config.setRaftStatus(raftStatus);
        config.setIoExecutor(MockExecutors.ioExecutor());

        raftLog = new DefaultRaftLog(config, statusManager);
        raftLog.idxItemsPerFile = 8;
        raftLog.idxMaxCacheItems = 2;
        raftLog.logFileSize = 1024;
        raftLog.logWriteBufferSize = 512;
        raftLog.init(() -> false);
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
        raftLog.append(Arrays.asList(items));

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
        raftLog.append(Arrays.asList(items));
        // replace
        items = FileLogLoaderTest.createItems(config, 4, new int[]{200, 200}, new int[]{100, 100});
        raftLog.append(Arrays.asList(items));

        it = raftLog.openIterator(() -> false);
        assertEquals(5, it.next(1, 5, 1000000).get().size());
        it.close();

        tearDown();

        // test recover
        init();
    }

    @Test
    public void testDelete() throws Exception {
        init();
        int[] totalSizes = new int[]{400, 400, 512, 200, 1024};
        int[] bizHeaderLen = new int[]{1, 0, 400, 100, 0};
        LogItem[] items = FileLogLoaderTest.createItems(config, 1, totalSizes, bizHeaderLen);
        raftLog.append(Arrays.asList(items));
        raftStatus.setCommitIndex(3);
        raftStatus.setLastApplied(3);
        raftStatus.setLastLogIndex(3);

        // test delete
        raftLog.markTruncateByIndex(3, 0);
        raftStatus.getTs().updateForUnitTest(System.nanoTime() + Duration.ofHours(1).toNanos(),
                System.currentTimeMillis() + Duration.ofHours(1).toMillis());
        raftLog.doDelete();
    }
}
