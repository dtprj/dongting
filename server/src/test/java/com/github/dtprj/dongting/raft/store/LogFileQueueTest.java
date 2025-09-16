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

import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.RunnableEx;
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.InitFiberFrame;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
@SuppressWarnings("SameParameterValue")
public class LogFileQueueTest extends BaseFiberTest {
    private LogFileQueue logFileQueue;
    private File dir;
    private RaftGroupConfigEx config;
    private RaftStatusImpl raftStatus;

    private int index;
    private int term;
    private int prevTerm;
    private int bizHeaderLen;

    private final HashMap<Long, Long> idxMap = new HashMap<>();
    private RuntimeException mockLoadEx;

    private final IdxOps idxOps = new IdxOps() {
        @Override
        public void put(long index, long position) {
            idxMap.put(index, position);
        }

        @Override
        public boolean needWaitFlush() {
            return false;
        }

        @Override
        public FiberFrame<Void> waitFlush() {
            return FiberFrame.voidCompletedFrame();
        }

        @Override
        public FiberFrame<Long> loadLogPos(long itemIndex) {
            if (mockLoadEx == null) {
                return FiberFrame.completedFrame(idxMap.get(itemIndex));
            } else {
                return FiberFrame.failedFrame(mockLoadEx);
            }
        }
    };

    private void setup(long fileSize, int maxWriteBufferSize) throws Exception {
        index = 1;
        term = 1;
        prevTerm = 0;
        bizHeaderLen = 64;
        mockLoadEx = null;

        idxMap.clear();

        dir = TestDir.createTestDir(LogFileQueueTest.class.getSimpleName());
        raftStatus = new RaftStatusImpl(0, dispatcher.ts);
        RaftServerConfig serverConfig = new RaftServerConfig();

        config = new RaftGroupConfigEx(1, "1", "1");
        config.blockIoExecutor = MockExecutors.ioExecutor();
        config.fiberGroup = fiberGroup;
        config.ts = raftStatus.ts;
        config.raftStatus = raftStatus;

        logFileQueue = new LogFileQueue(dir, config, idxOps, fileSize);
        logFileQueue.maxWriteBufferSize = maxWriteBufferSize;
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Throwable {
                InitFiberFrame.initRaftStatus(raftStatus, fiberGroup, serverConfig);
                logFileQueue.initQueue();
                FiberFrame<Integer> f = logFileQueue.restore(1, 0, 0);
                return Fiber.call(f, this::afterRestore);
            }

            private FrameCallResult afterRestore(Integer i) {
                logFileQueue.startFibers();
                return Fiber.frameReturn();
            }
        });
    }

    @AfterEach
    public void tearDown() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (logFileQueue != null) {
                    return logFileQueue.close().await(this::justReturn);
                } else {
                    return Fiber.frameReturn();
                }
            }
        });
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

    static LogItem createItem(RaftGroupConfigEx config, int term, int prevTerm, long index, int totalSize, int bizHeaderLen) {
        LogItem item = new LogItem();
        item.setType(1);
        item.setBizType(2);
        item.setTerm(term);
        item.setPrevLogTerm(prevTerm);
        item.setIndex(index);
        item.setTimestamp(config.ts.wallClockMillis);
        byte[] bs = new byte[bizHeaderLen];
        for (int i = 0; i < bizHeaderLen; i++) {
            bs[i] = (byte) i;
        }
        item.setHeader(new ByteArray(bs));
        int bodySize = totalSize - LogHeader.computeTotalLen(0, bizHeaderLen, 0);
        if (bodySize > 0) {
            // crc 4 bytes
            bodySize -= 4;
        }
        bs = new byte[bodySize];
        for (int i = 0; i < bodySize; i++) {
            bs[i] = (byte) i;
        }
        item.setBody(new ByteArray(bs));

        return item;
    }

    private void append(boolean check, long startPos, int... totalSizes) throws Exception {
        long fileSize = 1024;
        LogItem[] items = new LogItem[totalSizes.length];
        List<LogItem> list = new ArrayList<>();
        for (int i = 0; i < totalSizes.length; i++) {
            items[i] = createItem(config, term, prevTerm, index, totalSizes[i], bizHeaderLen);
            index++;
            prevTerm = term;
            list.add(items[i]);
        }

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(logFileQueue.append(list), v -> waitWriteFinish(null));
            }

            private FrameCallResult waitWriteFinish(Void v) {
                if (raftStatus.lastForceLogIndex < index - 1) {
                    return raftStatus.logForceFinishCondition.await(1000, this::waitWriteFinish);
                } else {
                    return Fiber.frameReturn();
                }
            }
        });

        assertEquals(items[items.length - 1].getIndex(), raftStatus.lastForceLogIndex);

        if (!check) {
            return;
        }
        ByteBuffer buf = load(startPos);
        CRC32C crc32C = new CRC32C();
        for (int i = 0; i < totalSizes.length; i++) {
            int len = totalSizes[i];
            if (len > buf.remaining()) {
                if (buf.remaining() >= LogHeader.ITEM_HEADER_SIZE) {
                    assertEquals(0xF19A7BCB, buf.getInt());
                }

                startPos += fileSize;
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
                    assertEquals(((ByteArray) item.getHeader()).getData()[j], buf.get());
                }
                crc32C.reset();
                RaftUtil.updateCrc(crc32C, buf, buf.position() - bizHeaderLen, bizHeaderLen);
                assertEquals((int) crc32C.getValue(), buf.getInt());
            }

            if (header.bodyLen > 0) {
                int bodyLen = header.bodyLen;
                for (int j = 0; j < bodyLen; j++) {
                    assertEquals(((ByteArray) item.getBody()).getData()[j], buf.get());
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
        setup(1024, 256);
        // test write buffer not enough
        append(true, 0L, 250, 250, 1024);
    }

    @Test
    public void testAppend3() throws Exception {
        setup(1024, 256);
        bizHeaderLen = 256 - LogHeader.ITEM_HEADER_SIZE - 4;
        // test write buffer full after write header
        append(true, 0L, 500, 500);
    }

    @Test
    public void testAppend4() throws Exception {
        setup(1024, 256);
        bizHeaderLen = 256 - LogHeader.ITEM_HEADER_SIZE - 4;
        bizHeaderLen += 1;
        // test write buffer has no space for header crc
        append(true, 0L, 500, 500);
    }

    @Test
    public void testAppend5() throws Exception {
        setup(1024, 256);
        bizHeaderLen = 256 - LogHeader.ITEM_HEADER_SIZE + 50;
        // not enough space for header
        append(true, 0L, 500, 500);
    }

    @Test
    public void testAppend6() throws Exception {
        setup(1024, 256);
        // zero biz header len
        bizHeaderLen = 0;
        append(true, 0L, 512, 512, 1024, 200);
    }

    @Test
    public void testAppend7() throws Exception {
        setup(1024, 256);
        // zero biz body len
        int len = LogHeader.ITEM_HEADER_SIZE + bizHeaderLen + 4;
        append(true, 0L, len, len, 1024 - len - len, 500);
    }

    @Test
    public void testAppend8() throws Exception {
        setup(1024, 256);
        // not enough space for body crc
        int len = 257;
        append(true, 0L, len, len, 1024 - len - len, 500);
    }

    @Test
    public void testRestore1() throws Exception {
        setup(1024, 1024);
        // last file not finished
        append(false, 0L, 200, 200, 1023, 500);
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return logFileQueue.close().await(this::afterClose);
            }

            private FrameCallResult afterClose(Void unused) throws IOException {
                logFileQueue = new LogFileQueue(dir, config, idxOps, 1024);
                logFileQueue.initQueue();
                assertThrows(RaftException.class, () -> logFileQueue.restore(1, -1, 0));
                assertThrows(RaftException.class, () -> logFileQueue.restore(1, 5000, 0));
                FiberFrame<Integer> f = logFileQueue.restore(1, 0, 0);
                return Fiber.call(f, this::resume);
            }

            private FrameCallResult resume(Integer integer) {
                assertEquals(2048 + 500, logFileQueue.logAppender.nextPersistPos);
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testRestore2() throws Exception {
        setup(1024, 1024);
        // last file finished
        append(false, 0L, 200, 1024);
        closeThenRestore(1024, 3, 2048);
    }

    private void closeThenRestore(int maxWriteBufferSize, long expectIndex, long expectPos) throws Exception {
        closeThenRestore(maxWriteBufferSize, expectIndex, expectPos, 1, 0, 0, null);
    }

    private void closeThenRestore(int maxWriteBufferSize, long expectIndex, long expectPos, long restoreIndex,
                                  long restorePos, long firstValidPos, RunnableEx<Exception> updater) throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return logFileQueue.close().await(this::afterClose);

            }

            private FrameCallResult afterClose(Void unused) throws Exception {
                if (updater != null) {
                    updater.run();
                }

                logFileQueue = new LogFileQueue(dir, config, idxOps, 1024);
                logFileQueue.maxWriteBufferSize = maxWriteBufferSize;
                logFileQueue.initQueue();
                FiberFrame<Integer> f = logFileQueue.restore(restoreIndex, restorePos, firstValidPos);
                return Fiber.call(f, this::resume);
            }

            private FrameCallResult resume(Integer integer) {
                assertEquals(expectPos, logFileQueue.logAppender.nextPersistPos);
                assertEquals(expectIndex, logFileQueue.logAppender.nextPersistIndex);
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testRestore3() throws Exception {
        setup(1024, 256);
        append(false, 0L, 300, 500, 1024);
        // small buffer
        closeThenRestore(256, 4, 2048);
    }

    @Test
    public void testRestore4() throws Exception {
        setup(1024, 256);
        append(false, 0L, 256, 256, 1024);
        // small buffer
        closeThenRestore(256, 4, 2048);
    }

    @Test
    public void testRestore5() throws Exception {
        setup(1024, 256);
        append(false, 0L, 255, 255, 1024);
        // small buffer
        closeThenRestore(256, 4, 2048);
    }

    @Test
    public void testRestore6() throws Exception {
        setup(1024, 256);
        append(false, 0L, 257, 257, 1024);
        // small buffer
        closeThenRestore(256, 4, 2048);
    }

    @Test
    public void testRestore7() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 200, 200, 1024);

        closeThenRestore(1024, 4, 2048, 3, 1024, 0, null);
        // restore from second file, first item of first file crc fail
        closeUpdateRestore(3, 1024, () -> {
            ByteBuffer data = load(0);
            data.putInt(0, data.getInt(0) + 1);
            write(0, data.array());
        }, ex -> assertInstanceOf(RaftException.class, ex));
    }

    private void closeUpdateRestore(long restoreIndex, long restoreIndexPos,
                                    RunnableEx<Exception> updater,
                                    Consumer<Throwable> exAssert) throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return logFileQueue.close().await(this::justReturn);
            }
        });
        if (updater != null) {
            updater.run();
        }

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Throwable {
                logFileQueue = new LogFileQueue(dir, config, idxOps, 1024);
                logFileQueue.initQueue();
                return Fiber.frameReturn();
            }
        });
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                FiberFrame<Integer> f = logFileQueue.restore(restoreIndex, restoreIndexPos, 0);
                return Fiber.call(f, this::resume);
            }

            private FrameCallResult resume(Integer integer) {
                throw new AssertionError();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                exAssert.accept(ex);
                return Fiber.frameReturn();
            }
        });
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
        // log header check fail

        closeUpdateRestore(1, 0, () -> updateHeader(0, 0, h -> {
            // now length exceed file size
            h.totalLen++;
            h.bodyLen++;
        }), e -> assertTrue(e.getMessage().startsWith("header check fail")));
    }

    @Test
    public void testRestore9() throws Exception {
        // prevLogTerm not match
        setup(1024, 1024);
        append(false, 0L, 200, 200);
        closeThenRestore(1024, 2, 200, 1, 0, 0,
                () -> updateHeader(0, 200, h -> h.prevLogTerm--));
    }

    @Test
    public void testRestore10() throws Exception {
        // index not match
        setup(1024, 1024);
        append(false, 0L, 200, 200);
        closeThenRestore(1024, 2, 200, 1, 0, 0,
                () -> updateHeader(0, 200, h -> h.index--));
    }

    @Test
    public void testRestore11() throws Exception {
        // term less than previous term
        setup(1024, 1024);
        append(false, 0L, 200, 200);
        closeThenRestore(1024, 2, 200, 1, 0, 0,
                () -> updateHeader(0, 200, h -> h.term = h.prevLogTerm - 1));
    }

    @Test
    public void testRestore12() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 200, 200);
        closeUpdateRestore(2, 0, null,
                e -> assertTrue(e.getMessage().startsWith("restoreIndex not match")));
    }

    @Test
    public void testRestore13() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 200, 200);
        closeUpdateRestore(1, 0, () -> updateHeader(0, 0, h -> h.term = 0),
                e -> assertTrue(e.getMessage().startsWith("invalid term")));
    }

    @Test
    public void testRestore14() throws Exception {
        setup(1024, 1024);
        // zero biz header len
        bizHeaderLen = 0;
        append(false, 0L, 200, 200);
        closeThenRestore(1024, 3, 400, 1, 0, 0, null);
    }

    @Test
    public void testRestore15() throws Exception {
        setup(1024, 1024);
        // zero biz body len
        int len = LogHeader.ITEM_HEADER_SIZE + bizHeaderLen + 4;
        append(false, 0L, len, len);
        closeThenRestore(1024, 3, len + len, 1, 0, 0, null);
    }

    @Test
    public void testRestore16() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 200, 300, 300);

        closeThenRestore(1024, 3, 500, 1, 0, 0, () -> {
            ByteBuffer buf = load(0);
            int bizHeaderCrcPos = 500 + LogHeader.ITEM_HEADER_SIZE + bizHeaderLen;
            buf.putInt(bizHeaderCrcPos, buf.getInt(bizHeaderCrcPos) + 1);
            write(0, buf.array());
        });
    }

    @Test
    public void testRestore17() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 500, 500, 200, 200);
        closeThenRestore(1024, 5, 1424, 4, 1224, 500, null);
        LogFile lf = logFileQueue.getLogFile(0);
        assertEquals(2, lf.firstIndex);

        closeThenRestore(1024, 5, 1424, 4, 1224, 1000, null);
        assertNull(logFileQueue.getLogFile(0));
        assertEquals(3, logFileQueue.getLogFile(1024).firstIndex);
    }

    @Test
    public void testRestore18() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 500, 400, 200, 200);
        closeThenRestore(1024, 5, 1424, 4, 1224, 900, null);
        assertNull(logFileQueue.getLogFile(0));
        assertEquals(3, logFileQueue.getLogFile(1024).firstIndex);
    }

    @Test
    public void testRestore19() throws Exception {
        setup(1024, 1024);
        append(false, 0L, 1020);
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(logFileQueue.ensureWritePosReady(1024), this::justReturn);
            }
        });
        closeThenRestore(1024, 2, 1024, 1, 0, 0, null);
    }

}
