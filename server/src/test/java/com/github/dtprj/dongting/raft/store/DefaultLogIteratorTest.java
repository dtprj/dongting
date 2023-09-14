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
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Arrays.asList;

/**
 * @author huangli
 */
public class DefaultLogIteratorTest {
    private LogFileQueue logFileQueue;
    private IdxFileQueue idxFileQueue;
    private RaftGroupConfigEx config;
    private StatusManager statusManager;
    private File dir;

    private LogItem[] items;

    private void setup(long fileSize) throws Exception {
        items = null;

        dir = TestDir.createTestDir(LogFileQueueTest.class.getSimpleName());
        config = new RaftGroupConfigEx(1, "1", "1");
        config.setRaftExecutor(MockExecutors.raftExecutor());
        config.setStopIndicator(() -> false);
        config.setTs(new Timestamp());
        config.setDirectPool(TwoLevelPool.getDefaultFactory().apply(config.getTs(), true));
        config.setHeapPool(new RefBufferFactory(TwoLevelPool.getDefaultFactory().apply(config.getTs(), false), 0));
        RaftStatusImpl raftStatus = new RaftStatusImpl();
        statusManager = new StatusManager(MockExecutors.ioExecutor(), raftStatus);
        statusManager.initStatusFileChannel(dir.getPath(), "test.status");
        config.setRaftStatus(raftStatus);
        config.setIoExecutor(MockExecutors.ioExecutor());

        idxFileQueue = new IdxFileQueue(dir, statusManager, config, 8, 2);
        logFileQueue = new LogFileQueue(dir, config, idxFileQueue, fileSize, 1024);
        idxFileQueue.init();
        Pair<Long, Long> p = idxFileQueue.initRestorePos();
        logFileQueue.init();
        logFileQueue.restore(p.getLeft(), p.getRight(), () -> false);
    }

    @AfterEach
    public void tearDown() {
        statusManager.close();
        idxFileQueue.close();
        logFileQueue.close();
    }

    private void append(int index, int[] totalSizes, int[] bizHeaderLens) throws Exception {
        items = new LogItem[totalSizes.length];
        for (int i = 0; i < totalSizes.length; i++) {
            int totalSize = totalSizes[i];
            int bizHeaderLen = bizHeaderLens[i];
            LogItem item = new LogItem(config.getDirectPool());
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
        logFileQueue.append(asList(items));
    }

    @Test
    public void testNext1() throws Exception {
        setup(1024);
        int[] totalSizes = new int[]{400, 400, 512, 512, 512, 500, 512, 508, 1024, 500};
        int[] bizHeaderLen = new int[]{1, 0, 512 - LogHeader.ITEM_HEADER_SIZE - 4, 20, 20, 20, 20, 20, 20, 20};
        append(1, totalSizes, bizHeaderLen);
        DefaultLogIterator it = new DefaultLogIterator(idxFileQueue, logFileQueue, config, () -> false, 200);
        CompletableFuture<List<LogItem>> f = it.next(1, totalSizes.length, 100000000);
        f.get();
        it.close();
    }
}
