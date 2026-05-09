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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.test.TestDir;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class MmapIoTaskTest extends BaseFiberTest {
    private static File dir;
    private File file;
    private LogFile logFile;
    private RaftGroupConfigEx groupConfig;

    @BeforeAll
    public static void setupDir() {
        dir = TestDir.createTestDir(MmapIoTaskTest.class.getSimpleName());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterAll
    public static void cleanDir() {
        if (dir.exists()) {
            dir.delete();
        }
    }

    @BeforeEach
    public void setup() throws Exception {
        file = new File(dir, "testFile");
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(4096);
        raf.close();
        Set<OpenOption> s = Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        logFile = new LogFile(0, 4096, file, fiberGroup, s, MockExecutors.ioExecutor(), null, 0);
        logFile.syncOpen();
        groupConfig = new RaftGroupConfigEx(1, "1", "");
        groupConfig.ioRetryInterval = new int[]{1};
        groupConfig.fiberGroup = fiberGroup;
        groupConfig.blockIoExecutor = MockExecutors.ioExecutor();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterEach
    public void clean() {
        if (file != null && file.exists()) {
            logFile.destroy();
            file.delete();
        }
    }

    @Test
    public void testRW() throws Exception {
        doInFiber(new FiberFrame<>() {
            ByteBuffer writeBuf = ByteBuffer.allocate(100);
            ByteBuffer readBuf;

            @Override
            public FrameCallResult execute(Void input) {
                writeBuf.putInt(12345);
                writeBuf.flip();
                MmapIoTask t = new MmapIoTask(fiberGroup, logFile);
                return t.run(new SingleBufferCallback(writeBuf, 0, true)).await(1000, this::resume1);
            }

            private FrameCallResult resume1(Void unused) {
                readBuf = ByteBuffer.allocate(4);
                MmapIoTask t = new MmapIoTask(fiberGroup, logFile);
                return t.run(new SingleBufferCallback(readBuf, 0, false)).await(1000, this::resume2);
            }

            private FrameCallResult resume2(Void unused) {
                readBuf.flip();
                assertEquals(12345, readBuf.getInt());
                return Fiber.frameReturn();
            }
        });
    }

    private void assertReadSuccess(Supplier<MmapIoTask> taskSupplier, IoCallback callback) throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                MmapIoTask t = taskSupplier.get();
                return t.run(callback).await(1000, this::justReturn);
            }
        });
    }

    private void assertReadFail(Supplier<MmapIoTask> taskSupplier, IoCallback callback) throws Exception {
        doInFiber(new FiberFrame<>() {
            MmapIoTask t;

            @Override
            public FrameCallResult execute(Void input) {
                t = taskSupplier.get();
                return t.run(callback).await(1000, this::resume);
            }

            private FrameCallResult resume(Void unused) {
                throw new AssertionError();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                assertSame(MOCK_EX, DtUtil.rootCause(ex));
                return Fiber.frameReturn();
            }
        });
    }

    private static final IOException MOCK_EX = new IOException("mock error");

    private static class FailCallback implements IoCallback {
        private final int failCount;
        private int count;

        FailCallback(int failCount) {
            this.failCount = failCount;
        }

        @Override
        public void run(ByteBuffer mmapBuffer) throws IOException {
            if (count++ < failCount) {
                throw MOCK_EX;
            }
        }
    }

    @Test
    public void testRetry() throws Exception {
        // fail on first attempt, retry succeeds
        assertReadSuccess(
                () -> new MmapIoTask(groupConfig.fiberGroup, logFile, groupConfig.ioRetryInterval, false, null),
                new FailCallback(1));

        // fail twice, retry exhausted
        assertReadFail(
                () -> new MmapIoTask(groupConfig.fiberGroup, logFile, groupConfig.ioRetryInterval, false, null),
                new FailCallback(2));

        // fail twice but retry forever
        assertReadSuccess(
                () -> new MmapIoTask(groupConfig.fiberGroup, logFile, groupConfig.ioRetryInterval, true, () -> false),
                new FailCallback(2));

        // fail, cancel indicator returns true
        assertReadFail(
                () -> new MmapIoTask(groupConfig.fiberGroup, logFile, groupConfig.ioRetryInterval, true, () -> true),
                new FailCallback(2));

        // cancel indicator returns true on second check
        AtomicInteger cancelIndicatorCount = new AtomicInteger();
        assertReadFail(
                () -> new MmapIoTask(groupConfig.fiberGroup, logFile, groupConfig.ioRetryInterval, true,
                        () -> cancelIndicatorCount.getAndIncrement() == 1),
                new FailCallback(2));

        // no retry configured
        assertReadFail(
                () -> new MmapIoTask(groupConfig.fiberGroup, logFile, null, false, null),
                new FailCallback(1));
    }

    @Test
    public void testAsyncOpen() throws Exception {
        logFile.destroy();
        file.delete();

        file = new File(dir, "testAsyncOpen");
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(4096);
        raf.close();
        Set<OpenOption> s = Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        logFile = new LogFile(0, 4096, file, fiberGroup, s, MockExecutors.ioExecutor(), null, 0);

        doInFiber(new FiberFrame<>() {
            ByteBuffer buf = ByteBuffer.allocate(8);

            @Override
            public FrameCallResult execute(Void input) {
                assertTrue(!logFile.isOpen());
                MmapIoTask t = new MmapIoTask(fiberGroup, logFile);
                return t.run(new SingleBufferCallback(buf, 0, true)).await(1000, this::resume1);
            }

            private FrameCallResult resume1(Void unused) {
                assertTrue(logFile.isOpen());
                buf = ByteBuffer.allocate(8);
                MmapIoTask t = new MmapIoTask(fiberGroup, logFile);
                return t.run(new SingleBufferCallback(buf, 0, false)).await(1000, this::resume2);
            }

            private FrameCallResult resume2(Void unused) {
                assertTrue(logFile.isOpen());
                return Fiber.frameReturn();
            }
        });
    }
}
