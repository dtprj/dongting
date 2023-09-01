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
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * @author huangli
 */
public class LogFileQueueTest {
    private LogFileQueue logFileQueue;
    private RaftStatusImpl raftStatus;
    private StatusManager statusManager;
    private File dir;
    private RaftGroupConfigEx config;

    private void setup(long fileSize, int writeBufferSize) throws Exception {
        dir = TestDir.createTestDir(LogFileQueueTest.class.getSimpleName());
        config = new RaftGroupConfigEx(1, "1", "1");
        config.setRaftExecutor(MockExecutors.raftExecutor());
        config.setStopIndicator(() -> false);
        config.setTs(new Timestamp());
        config.setDirectPool(TwoLevelPool.getDefaultFactory().apply(config.getTs(), true));
        config.setHeapPool(new RefBufferFactory(TwoLevelPool.getDefaultFactory().apply(config.getTs(), false), 0));
        raftStatus = new RaftStatusImpl();
        statusManager = new StatusManager(MockExecutors.ioExecutor(), raftStatus);
        statusManager.initStatusFileChannel(dir.getPath(), "test.status");
        config.setRaftStatus(raftStatus);
        config.setIoExecutor(MockExecutors.ioExecutor());
        IdxOps idxOps = new IdxOps() {

            @Override
            public void put(long index, long position, boolean recover) {
            }

            @Override
            public long syncLoadLogPos(long itemIndex) {
                return (itemIndex - 1) * 100;
            }
        };
        logFileQueue = new LogFileQueue(dir, config, idxOps, fileSize, writeBufferSize);
        logFileQueue.init();
        logFileQueue.restore(1, 0, () -> false);
    }

    @AfterEach
    public void tearDown() {
        logFileQueue.close();
    }

    private LogItem createItem(int totalSize) {
        LogItem item = new LogItem(config.getDirectPool());
        item.setType(1);
        item.setBizType(2);
        item.setTerm(1000);
        item.setPrevLogTerm(2000);
        item.setIndex(3000);
        item.setTimestamp(Long.MAX_VALUE);
        item.setHeaderBuffer(ByteBuffer.allocate(64));
        int bodySize = totalSize - LogHeader.computeTotalLen(0, 64, 0) - 4;
        item.setBodyBuffer(ByteBuffer.allocate(bodySize));
        return item;
    }

    @Test
    public void testAppend1() throws Exception {
        setup(1024, 4000);
        logFileQueue.append(singletonList(createItem(1023)));
        logFileQueue.append(singletonList(createItem(1024)));
        logFileQueue.append(asList(createItem(511), createItem(200), createItem(1024)));
        logFileQueue.append(asList(createItem(512), createItem(512), createItem(1024)));
        logFileQueue.append(asList(createItem(512), createItem(511), createItem(1024)));
    }

    @Test
    public void testAppend2() throws Exception {
        setup(1024, 200);
        // test write buffer not enough
        logFileQueue.append(asList(createItem(190), createItem(190), createItem(1024)));
    }

    @Test
    public void testAppend3() throws Exception {
        setup(1024, LogHeader.ITEM_HEADER_SIZE + 64 + 4);
        // test write buffer full after write header
        logFileQueue.append(asList(createItem(190), createItem(190)));
    }

    @Test
    public void testAppend4() throws Exception {
        setup(1024, LogHeader.ITEM_HEADER_SIZE + 64 + 3);
        // test write buffer has no space for header crc
        logFileQueue.append(asList(createItem(190), createItem(190)));
    }
}
