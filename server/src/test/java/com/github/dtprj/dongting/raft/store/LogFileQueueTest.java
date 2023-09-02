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
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
        ByteBuffer buf = ByteBuffer.allocate(64);
        for (int i = 0; i < 64; i++) {
            buf.put((byte) i);
        }
        buf.clear();
        item.setHeaderBuffer(buf);
        int bodySize = totalSize - LogHeader.computeTotalLen(0, 64, 0) - 4;
        buf = ByteBuffer.allocate(bodySize);
        for (int i = 0; i < bodySize; i++) {
            buf.put((byte) i);
        }
        buf.clear();
        item.setBodyBuffer(buf);
        return item;
    }

    private ByteBuffer load(long pos) throws Exception {
        File f = new File(dir, String.format("%020d", pos));
        try (FileInputStream fis = new FileInputStream(f)) {
            return ByteBuffer.wrap(fis.readAllBytes());
        }
    }

    private void append(long startPos, int... bodySizes) throws Exception {
        LogItem[] items = new LogItem[bodySizes.length];
        for (int i = 0; i < bodySizes.length; i++) {
            items[i] = createItem(bodySizes[i]);
        }
        logFileQueue.append(asList(items));
        ByteBuffer buf = load(startPos);
        CRC32C crc32C = new CRC32C();
        for (int i = 0; i < bodySizes.length; i++) {
            int len = bodySizes[i];
            if (len > buf.remaining()) {
                if (buf.remaining() >= LogHeader.ITEM_HEADER_SIZE) {
                    assertEquals(0xF19A7BCB, buf.getInt());
                }

                startPos += 1024;
                buf = load(startPos);
            }
            LogItem item = items[i];
            LogHeader header = new LogHeader();
            header.read(buf);
            assertEquals(item.getType(), header.type);
            assertEquals(item.getBizType(), header.bizType);
            assertEquals(item.getTerm(), header.term);
            assertEquals(item.getPrevLogTerm(), header.prevLogTerm);
            assertEquals(item.getIndex(), header.index);
            assertEquals(item.getTimestamp(), header.timestamp);

            for (int j = 0; j < 64; j++) {
                assertEquals(item.getHeaderBuffer().get(j), buf.get());
            }
            crc32C.reset();
            RaftUtil.updateCrc(crc32C, buf, buf.position() - 64, 64);
            assertEquals((int) crc32C.getValue(), buf.getInt());

            int bodyLen = item.getBodyBuffer().capacity();
            for (int j = 0; j < bodyLen; j++) {
                assertEquals(item.getBodyBuffer().get(j), buf.get());
            }
            crc32C.reset();
            RaftUtil.updateCrc(crc32C, buf, buf.position() - bodyLen, bodyLen);
            assertEquals((int) crc32C.getValue(), buf.getInt());
        }
    }

    @Test
    public void testAppend1() throws Exception {
        setup(1024, 4000);
        append(0L, 1023);
        append(1024L, 1024);
        append(2048L, 511, 200, 1024);
        append(4096L, 512, 512, 1024);
        append(6144L, 512, 511, 1024);
    }

    @Test
    public void testAppend2() throws Exception {
        setup(1024, 200);
        // test write buffer not enough
        append(0L, 190, 190, 1024);
    }

    @Test
    public void testAppend3() throws Exception {
        setup(1024, LogHeader.ITEM_HEADER_SIZE + 64 + 4);
        // test write buffer full after write header
        append(0L, 190, 190);
    }

    @Test
    public void testAppend4() throws Exception {
        setup(1024, LogHeader.ITEM_HEADER_SIZE + 64 + 3);
        // test write buffer has no space for header crc
        append(0L, 190, 190);
    }
}
