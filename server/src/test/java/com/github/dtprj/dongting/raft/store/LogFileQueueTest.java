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
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

import static com.github.dtprj.dongting.raft.test.TestUtil.getResultInExecutor;
import static com.github.dtprj.dongting.raft.test.TestUtil.waitUtilInExecutor;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
public class LogFileQueueTest {
    private LogFileQueue logFileQueue;
    private RaftStatusImpl raftStatus;
    private StatusManager statusManager;
    private File dir;
    private RaftGroupConfigEx config;

    private int index;
    private int term;
    private int prevTerm;
    private int bizHeaderLen;

    private final HashMap<Long, Long> idxMap = new HashMap<>();


    private final IdxOps idxOps = new IdxOps() {

        @Override
        public void put(long index, long position, boolean recover) {
            idxMap.put(index, position);
        }

        @Override
        public long syncLoadLogPos(long itemIndex) {
            return idxMap.get(itemIndex);
        }
    };

    private void setup(long fileSize, int writeBufferSize) throws Exception {
        index = 1;
        term = 1;
        prevTerm = 0;
        bizHeaderLen = 64;

        idxMap.clear();

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
        item.setTerm(term);
        item.setPrevLogTerm(prevTerm);
        prevTerm = term;
        item.setIndex(index++);
        item.setTimestamp(config.getTs().getWallClockMillis());
        ByteBuffer buf = ByteBuffer.allocate(bizHeaderLen);
        for (int i = 0; i < bizHeaderLen; i++) {
            buf.put((byte) i);
        }
        buf.clear();
        item.setHeaderBuffer(buf);
        int bodySize = totalSize - LogHeader.computeTotalLen(0, bizHeaderLen, 0);
        if (bodySize > 0) {
            // crc 4 bytes
            bodySize -= 4;
        }
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

    private void write(long pos, byte[] data) throws Exception {
        File f = new File(dir, String.format("%020d", pos));
        try (FileOutputStream fos = new FileOutputStream(f)) {
            fos.write(data);
        }
    }

    private void append(boolean check, long startPos, int... bodySizes) throws Exception {
        LogItem[] items = new LogItem[bodySizes.length];
        for (int i = 0; i < bodySizes.length; i++) {
            items[i] = createItem(bodySizes[i]);
        }
        logFileQueue.append(asList(items));
        if (!check) {
            return;
        }
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
            assertTrue(header.crcMatch());
            assertEquals(item.getType(), header.type);
            assertEquals(item.getBizType(), header.bizType);
            assertEquals(item.getTerm(), header.term);
            assertEquals(item.getPrevLogTerm(), header.prevLogTerm);
            assertEquals(item.getIndex(), header.index);
            assertEquals(item.getTimestamp(), header.timestamp);

            if (bizHeaderLen > 0) {
                for (int j = 0; j < bizHeaderLen; j++) {
                    assertEquals(item.getHeaderBuffer().get(j), buf.get());
                }
                crc32C.reset();
                RaftUtil.updateCrc(crc32C, buf, buf.position() - bizHeaderLen, bizHeaderLen);
                assertEquals((int) crc32C.getValue(), buf.getInt());
            }

            if (header.bodyLen > 0) {
                int bodyLen = header.bodyLen;
                for (int j = 0; j < bodyLen; j++) {
                    assertEquals(item.getBodyBuffer().get(j), buf.get());
                }
                crc32C.reset();
                RaftUtil.updateCrc(crc32C, buf, buf.position() - bodyLen, bodyLen);
                assertEquals((int) crc32C.getValue(), buf.getInt());
            }
        }
    }

    @Test
    public void testAppend1() throws Exception {
        setup(1024, 4000);
        append(true, 0L, 1023);
        append(true, 1024L, 1024);
        append(true, 2048L, 511, 200, 1024);
        append(true, 4096L, 512, 512, 1024);
        append(true, 6144L, 512, 511, 1024);
    }

    @Test
    public void testAppend2() throws Exception {
        setup(1024, 200);
        // test write buffer not enough
        append(true, 0L, 190, 190, 1024);
    }

    @Test
    public void testAppend3() throws Exception {
        setup(1024, LogHeader.ITEM_HEADER_SIZE + bizHeaderLen + 4);
        // test write buffer full after write header
        append(true, 0L, 190, 190);
    }

    @Test
    public void testAppend4() throws Exception {
        setup(1024, LogHeader.ITEM_HEADER_SIZE + bizHeaderLen + 3);
        // test write buffer has no space for header crc
        append(true, 0L, 190, 190);
    }

    @Test
    public void testAppend5() throws Exception {
        setup(1024, 200);
        // zero biz header len
        bizHeaderLen = 0;
        append(true, 0L, 190, 190);
    }

    @Test
    public void testAppend6() throws Exception {
        setup(1024, 200);
        // zero biz body len
        int len = LogHeader.ITEM_HEADER_SIZE + bizHeaderLen + 4;
        append(true, 0L, len, len);
    }

    @Test
    public void testAppend7() throws Exception {
        // use buffer equal to header size
        setup(1024, LogHeader.ITEM_HEADER_SIZE);
        append(true, 0L, 200, 200);
    }

    @Test
    public void testTruncateTail() throws Exception {
        setup(1024, 4000);
        append(false, 0L, 200, 200, 1024);
        logFileQueue.syncTruncateTail(200, 2048);
        ByteBuffer buf = load(0L);
        for (int i = 200; i < 400; i++) {
            assertEquals(0, buf.get(i));
        }
        buf = load(1024L);
        for (int i = 0; i < 1024; i++) {
            assertEquals(0, buf.get(i));
        }
        assertEquals(200, logFileQueue.getWritePos());
    }

    @Test
    public void testDelete() throws Exception {
        setup(1024, 4000);
        append(false, 0L, 200, 200, 1024);
        logFileQueue.markDelete(3, Long.MAX_VALUE, 1000);
        logFileQueue.submitDeleteTask(config.getTs().getWallClockMillis());
        Thread.sleep(10);
        assertEquals(0L, getResultInExecutor(config.getRaftExecutor(), () -> logFileQueue.queueStartPosition));
        logFileQueue.markDelete(3, Long.MAX_VALUE, 1);
        config.getTs().refresh(0);
        logFileQueue.submitDeleteTask(config.getTs().getWallClockMillis());
        waitUtilInExecutor(config.getRaftExecutor(), 1024L, () -> logFileQueue.queueStartPosition);
    }

    @Test
    public void testRestore1() throws Exception {
        setup(1024, 4000);
        // last file not finished
        append(false, 0L, 200, 200, 1023, 500);
        logFileQueue.close();
        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 4000);
        logFileQueue.init();
        assertThrows(RaftException.class, () -> logFileQueue.restore(1, -1, () -> false));
        assertThrows(RaftException.class, () -> logFileQueue.restore(1, 5000, () -> false));
        logFileQueue.restore(1, 0, () -> false);
        assertEquals(2048 + 500, logFileQueue.getWritePos());
    }

    @Test
    public void testRestore2() throws Exception {
        setup(1024, 4000);
        // last file finished
        append(false, 0L, 200, 1024);
        logFileQueue.close();
        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 4000);
        logFileQueue.init();
        logFileQueue.restore(1, 0, () -> false);
        assertEquals(2048, logFileQueue.getWritePos());
    }

    @Test
    public void testRestore3() throws Exception {
        setup(1024, 4000);
        append(false, 0L, 200, 200, 1024);
        logFileQueue.close();
        // small buffer
        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 190);
        logFileQueue.init();
        logFileQueue.restore(1, 0, () -> false);
        assertEquals(2048, logFileQueue.getWritePos());
    }

    @Test
    public void testRestore4() throws Exception {
        setup(1024, 4000);
        append(false, 0L, 200, 200, 1024);
        logFileQueue.close();
        // small buffer
        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 200);
        logFileQueue.init();
        logFileQueue.restore(1, 0, () -> false);
        assertEquals(2048, logFileQueue.getWritePos());
    }

    @Test
    public void testRestore5() throws Exception {
        setup(1024, 4000);
        append(false, 0L, 200, 200, 1024);
        logFileQueue.close();
        // small buffer
        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 201);
        logFileQueue.init();
        logFileQueue.restore(1, 0, () -> false);
        assertEquals(2048, logFileQueue.getWritePos());
    }

    @Test
    public void testRestore6() throws Exception {
        setup(1024, 4000);
        append(false, 0L, 200, 200, 1024);
        logFileQueue.close();
        // small buffer
        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 199);
        logFileQueue.init();
        logFileQueue.restore(1, 0, () -> false);
        assertEquals(2048, logFileQueue.getWritePos());
    }

    @Test
    public void testRestore7() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 200, 200, 1024);
        logFileQueue.close();

        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 1024);
        logFileQueue.init();
        // restore from second file
        logFileQueue.restore(3, 1024, () -> false);
        assertEquals(2048, logFileQueue.getWritePos());
        logFileQueue.close();


        ByteBuffer data = load(0);
        data.putInt(0, data.getInt(0) + 1);
        write(0, data.array());
        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 1024);
        logFileQueue.init();
        // restore from second file, first item of first file crc fail
        assertThrows(RaftException.class, () -> logFileQueue.restore(3, 1024, () -> false));
    }

    private void updateHeader(int filePos, int offset, Consumer<LogHeader> headerUpdater) throws Exception {
        ByteBuffer buf = load(filePos);
        LogHeader header = new LogHeader();
        buf.position(offset);
        header.read(buf);
        headerUpdater.accept(header);
        buf.position(offset);
        buf.putInt(header.totalLen);
        buf.putInt(header.contextLen);
        buf.putInt(header.bizHeaderLen);
        buf.putInt(header.bodyLen);
        buf.put((byte) header.type);
        buf.put((byte) header.bizType);
        buf.putInt(header.term);
        buf.putInt(header.prevLogTerm);
        buf.putLong(header.index);
        buf.putLong(header.timestamp);
        CRC32C c = new CRC32C();
        RaftUtil.updateCrc(c, buf, offset, LogHeader.ITEM_HEADER_SIZE - 4);
        buf.putInt((int) c.getValue());
        write(filePos, buf.array());
    }

    @Test
    public void testRestore8() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 1024);
        logFileQueue.close();

        updateHeader(0, 0, h -> {
            // now length exceed file size
            h.totalLen++;
            h.bodyLen++;
        });

        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 1024);
        logFileQueue.init();
        try {
            // log header check fail
            logFileQueue.restore(1, 0, () -> false);
            fail();
        } catch (RaftException e) {
            assertTrue(e.getMessage().startsWith("header check fail"));
        }
    }

    @Test
    public void testRestore9() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 200, 200);
        logFileQueue.close();
        updateHeader(0, 200, h -> h.prevLogTerm--);

        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 1024);
        logFileQueue.init();
        try {
            logFileQueue.restore(1, 0, () -> false);
            fail();
        } catch (RaftException e) {
            assertTrue(e.getMessage().startsWith("prevLogTerm not match"));
        }
    }

    @Test
    public void testRestore10() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 200, 200);
        logFileQueue.close();
        updateHeader(0, 200, h -> h.index++);

        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 1024);
        logFileQueue.init();
        try {
            logFileQueue.restore(1, 0, () -> false);
            fail();
        } catch (RaftException e) {
            assertTrue(e.getMessage().startsWith("index not match"));
        }
    }

    @Test
    public void testRestore11() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 200, 200);
        logFileQueue.close();
        updateHeader(0, 200, h -> h.term = h.prevLogTerm - 1);

        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 1024);
        logFileQueue.init();
        try {
            logFileQueue.restore(1, 0, () -> false);
            fail();
        } catch (RaftException e) {
            assertTrue(e.getMessage().startsWith("term less than previous term"));
        }
    }

    @Test
    public void testRestore12() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 200, 200);
        logFileQueue.close();

        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 1024);
        logFileQueue.init();
        try {
            logFileQueue.restore(2, 0, () -> false);
            fail();
        } catch (RaftException e) {
            assertTrue(e.getMessage().startsWith("restoreIndex not match"));
        }
    }

    @Test
    public void testRestore13() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 200, 200);
        logFileQueue.close();
        updateHeader(0, 0, h -> h.term = 0);

        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 1024);
        logFileQueue.init();
        try {
            logFileQueue.restore(1, 0, () -> false);
            fail();
        } catch (RaftException e) {
            assertTrue(e.getMessage().startsWith("invalid term"));
        }
    }

    @Test
    public void testRestore14() throws Exception {
        setup(1024, 1024);
        // zero biz header len
        bizHeaderLen = 0;
        append(false, 0L, 200, 200);
        logFileQueue.close();
        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 1024);
        logFileQueue.init();
        logFileQueue.restore(1, 0, () -> false);
        assertEquals(400, logFileQueue.getWritePos());
    }

    @Test
    public void testRestore15() throws Exception {
        setup(1024, 1024);
        // zero biz body len
        int len = LogHeader.ITEM_HEADER_SIZE + bizHeaderLen + 4;
        append(false, 0L, len, len);
        logFileQueue.close();
        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 1024);
        logFileQueue.init();
        logFileQueue.restore(1, 0, () -> false);
        assertEquals(len + len, logFileQueue.getWritePos());
    }

    @Test
    public void testRestore16() throws Exception {
        setup(1024, 200);
        append(false, 0L, 201);
        logFileQueue.close();

        ByteBuffer buf = load(0);
        int bizHeaderCrcPos = LogHeader.ITEM_HEADER_SIZE + bizHeaderLen;
        buf.putInt(bizHeaderCrcPos, buf.getInt(bizHeaderCrcPos) + 1);
        write(0, buf.array());

        logFileQueue = new LogFileQueue(dir, config, idxOps, 1024, 1024);
        logFileQueue.init();
        try {
            logFileQueue.restore(1, 0, () -> false);
            fail();
        } catch (RaftException e) {
            assertTrue(e.getMessage().startsWith("restore index crc not match"));
        }
    }

    @Test
    public void testTryFindMatch() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 512, 512);
        term++;
        append(false, 0L, 512, 512);
        term++;
        append(false, 0L, 512, 512);
        term++;
        append(false, 0L, 300, 300, 300);
        append(false, 0L, 256, 256, 256, 256);
        checkMatch(1, 1);
        checkMatch(1, 2);
        checkMatch(2, 3);
        checkMatch(2, 4);
        checkMatch(3, 5);
        checkMatch(3, 6);
        checkMatch(4, 7);
        checkMatch(4, 8);
        checkMatch(4, 9);
        checkMatch(4, 10);
        checkMatch(4, 11);
        checkMatch(4, 12);
        checkMatch(4, 13);
        // allocate next empty file
        logFileQueue.ensureWritePosReady(logFileQueue.queueEndPosition);

        assertNull(logFileQueue.tryFindMatchPos(1, 0, () -> false).get());
        assertNull(logFileQueue.tryFindMatchPos(0, 1, () -> false).get());
        assertEquals(new Pair<>(4, 12L), logFileQueue.tryFindMatchPos(5, 13, () -> false).get());
        assertEquals(new Pair<>(3, 6L), logFileQueue.tryFindMatchPos(3, 13, () -> false).get());
        assertEquals(new Pair<>(3, 6L), logFileQueue.tryFindMatchPos(3, 1000, () -> false).get());
        assertThrows(CancellationException.class, () -> logFileQueue.tryFindMatchPos(1, 1, () -> true).get());


        logFileQueue.markDelete(3, Long.MAX_VALUE, 0);
        assertNull(logFileQueue.tryFindMatchPos(1, 2, () -> false).get());
        assertNull(logFileQueue.tryFindMatchPos(1, 1, () -> false).get());
        checkMatch(2, 3);
    }

    private void checkMatch(int suggestTerm, long suggestIndex) throws Exception {
        CompletableFuture<Pair<Integer, Long>> f = logFileQueue.tryFindMatchPos(suggestTerm, suggestIndex, () -> false);
        assertEquals(suggestTerm, f.get().getLeft());
        assertEquals(suggestIndex, f.get().getRight());
    }
}
