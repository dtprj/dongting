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
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.StoppedException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
@SuppressWarnings("SameParameterValue")
public class FileLogLoaderTest {
    private LogFileQueue logQueue;
    private IdxFileQueue idxQueue;
    private RaftGroupConfigEx config;
    private StatusManager statusManager;

    private LogItem[] items;

    private void setup(long fileSize) throws Exception {
        File dir = TestDir.createTestDir(LogFileQueueTest.class.getSimpleName());
        RaftStatusImpl raftStatus = new RaftStatusImpl();
        config = new RaftGroupConfigEx(1, "1", "1");
        config.setRaftExecutor(MockExecutors.raftExecutor());
        config.setIoExecutor(MockExecutors.ioExecutor());
        config.setStopIndicator(() -> false);
        config.setTs(raftStatus.getTs());
        config.setDirectPool(TwoLevelPool.getDefaultFactory().apply(config.getTs(), true));
        config.setHeapPool(new RefBufferFactory(TwoLevelPool.getDefaultFactory().apply(config.getTs(), false), 0));
        statusManager = new StatusManager(MockExecutors.ioExecutor(), raftStatus, dir.getPath(), "test.status");
        statusManager.initStatusFile();
        config.setRaftStatus(raftStatus);
        config.setIoExecutor(MockExecutors.ioExecutor());

        idxQueue = new IdxFileQueue(FileUtil.ensureDir(dir,"idx"), statusManager, config, 8, 4);
        logQueue = new LogFileQueue(FileUtil.ensureDir(dir,"log"), config, idxQueue, fileSize, 1024);
        idxQueue.init();
        Pair<Long, Long> p = idxQueue.initRestorePos();
        logQueue.init();
        logQueue.restore(p.getLeft(), p.getRight(), () -> false);
    }

    @AfterEach
    public void tearDown() {
        items = null;
        statusManager.close();
        idxQueue.close();
        logQueue.close();
    }

    private void append(int index, int[] totalSizes, int[] bizHeaderLens) throws Exception {
        items = createItems(config, index, totalSizes, bizHeaderLens);
        logQueue.append(asList(items));
    }

    public static LogItem[] createItems(RaftGroupConfigEx config, int index, int[] totalSizes, int[] bizHeaderLens) {
        LogItem[] items = new LogItem[totalSizes.length];
        for (int i = 0; i < totalSizes.length; i++) {
            int totalSize = totalSizes[i];
            int bizHeaderLen = bizHeaderLens[i];
            LogItem item = new LogItem(config.getHeapPool().getPool());
            item.setIndex(index + i);
            item.setType(1);
            item.setBizType(2);
            item.setTerm(100);
            item.setPrevLogTerm(100);
            item.setTimestamp(config.getTs().getWallClockMillis());
            ByteBuffer buf = ByteBuffer.allocate(bizHeaderLen);
            for (int j = 0; j < bizHeaderLen; j++) {
                buf.put((byte) j);
            }
            buf.clear();
            item.setHeaderBuffer(buf);
            int bodySize = totalSize - LogHeader.computeTotalLen(0, bizHeaderLen, 0);
            if (bodySize > 0) {
                // crc 4 bytes
                bodySize -= 4;
            }
            buf = ByteBuffer.allocate(bodySize);
            for (int j = 0; j < bodySize; j++) {
                buf.put((byte) j);
            }
            buf.clear();
            item.setBodyBuffer(buf);
            items[i] = item;
        }
        return items;
    }

    @Test
    public void testNext() throws Exception {
        setup(1024);
        int[] totalSizes = new int[]{400, 400, 512, 512, 512, 500, 512, 508, 1024, 500};
        int[] bizHeaderLen = new int[]{1, 0, 512 - LogHeader.ITEM_HEADER_SIZE - 4, 20, 20, 250, 250, 250, 250, 250};
        append(1, totalSizes, bizHeaderLen);
        try (FileLogLoader it = new FileLogLoader(idxQueue, logQueue, config, () -> false, 200)) {
            CompletableFuture<List<LogItem>> f = it.next(1, totalSizes.length, 100000000);
            assertNotNull(f);
            assertEquals(totalSizes.length, f.get().size());
        }
        // use large buffer, with limited items
        try (FileLogLoader it = new FileLogLoader(idxQueue, logQueue, config, () -> false, 2000)) {
            assertEquals(3, it.next(1, 3, 100000000).get().size());
            assertEquals(3, it.next(4, 3, 100000000).get().size());
            assertEquals(3, it.next(7, 3, 100000000).get().size());
            assertEquals(1, it.next(10, 1, 100000000).get().size());
        }
        // test bytes limit
        try (FileLogLoader it = new FileLogLoader(idxQueue, logQueue, config, () -> false, 300)) {
            assertEquals(3, it.next(1, 10, 800).get().size());
        }
        // test bytes limit
        try (FileLogLoader it = new FileLogLoader(idxQueue, logQueue, config, () -> false, 300)) {
            assertEquals(1, it.next(1, 10, 200).get().size());
        }
        // test close
        try (FileLogLoader it = new FileLogLoader(idxQueue, logQueue, config, () -> false, 300)) {
            it.close();
            assertThrows(ExecutionException.class, () -> it.next(1, 10, 2000000).get());
        }
        // test cancel
        try (FileLogLoader it = new FileLogLoader(idxQueue, logQueue, config, () -> true, 300)) {
            assertThrows(CancellationException.class, () -> it.next(1, 100, 2000000).get());
        }
        // test stop
        config.setStopIndicator(() -> true);
        try (FileLogLoader it = new FileLogLoader(idxQueue, logQueue, config, () -> false, 300)) {
            try {
                it.next(1, 10, 2000000).get();
                fail();
            } catch (Exception e) {
                assertEquals(StoppedException.class, DtUtil.rootCause(e).getClass());
            }
        }
    }
}
