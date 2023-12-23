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
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCall;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
@SuppressWarnings("SameParameterValue")
public class LogFileQueueTest extends BaseFiberTest {
    private LogFileQueue logFileQueue;
    private File dir;
    private RaftGroupConfig config;
    private TailCache tailCache;

    private int index;
    private int term;
    private int prevTerm;
    private int bizHeaderLen;

    private final HashMap<Long, Long> idxMap = new HashMap<>();
    private Throwable mockLoadEx;
    private static class AppendCallback implements RaftLog.AppendCallback {

        int lastPersistTerm;
        long lastPersistIndex;

        @Override
        public void finish(int lastPersistTerm, long lastPersistIndex) {
            this.lastPersistTerm = lastPersistTerm;
            this.lastPersistIndex = lastPersistIndex;
        }
    }
    private AppendCallback appendCallback;


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
        public FrameCallResult loadLogPos(long itemIndex, FrameCall<Long> resumePoint) throws Throwable {
            if (mockLoadEx == null) {
                return Fiber.resume(idxMap.get(itemIndex), resumePoint);
            } else {
                throw mockLoadEx;
            }
        }
    };

    private void setup(long fileSize, int maxWriteBufferSize) throws Exception {
        index = 1;
        term = 1;
        prevTerm = 0;
        bizHeaderLen = 64;
        mockLoadEx = null;
        appendCallback = new AppendCallback();

        idxMap.clear();

        dir = TestDir.createTestDir(LogFileQueueTest.class.getSimpleName());
        RaftStatusImpl raftStatus = new RaftStatusImpl();
        tailCache = new TailCache();
        raftStatus.setTailCache(tailCache);
        config = new RaftGroupConfig(1, "1", "1");
        config.setIoExecutor(MockExecutors.ioExecutor());
        config.setFiberGroup(fiberGroup);
        config.setTs(raftStatus.getTs());
        config.setDirectPool(TwoLevelPool.getDefaultFactory().apply(config.getTs(), true));
        config.setHeapPool(new RefBufferFactory(TwoLevelPool.getDefaultFactory().apply(config.getTs(), false), 0));
        config.setRaftStatus(raftStatus);
        logFileQueue = new LogFileQueue(dir, config, idxOps, appendCallback, fileSize);
        logFileQueue.maxWriteBufferSize = maxWriteBufferSize;
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Throwable {
                logFileQueue.init();
                FiberFrame<Integer> f = logFileQueue.restore(1, 0, 0);
                return Fiber.call(f, i-> Fiber.frameReturn());
            }
        });
    }

    @AfterEach
    public void tearDown() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                logFileQueue.close();
                return Fiber.frameReturn();
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

    private LogItem createItem(int totalSize) {
        LogItem item = new LogItem(config.getHeapPool().getPool());
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

    private void append(boolean check, long startPos, int... totalSizes) throws Exception {
        long fileSize = 1024;
        LogItem[] items = new LogItem[totalSizes.length];
        for (int i = 0; i < totalSizes.length; i++) {
            items[i] = createItem(totalSizes[i]);
            RaftInput ri = new RaftInput(0, null, null, null, 0);
            RaftTask rt = new RaftTask(config.getTs(), LogItem.TYPE_NORMAL, ri, null);
            rt.setItem(items[i]);
            tailCache.put(items[i].getIndex(), rt);
        }

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                logFileQueue.append();
                FiberFrame<Void> f = logFileQueue.logAppender.waitWriteFinishOrShouldStopOrClose();
                return Fiber.call(f, this::justReturn);
            }
        });

        assertEquals(items[items.length - 1].getIndex(), appendCallback.lastPersistIndex);
        assertEquals(items[items.length - 1].getTerm(), appendCallback.lastPersistTerm);

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

}
