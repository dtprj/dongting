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
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class AsyncIoTaskTest extends BaseFiberTest {
    private static File dir;
    private File file;
    private LogFile dtFile;
    private RaftGroupConfigEx groupConfig;

    @BeforeAll
    public static void setupDir() {
        dir = TestDir.createTestDir(AsyncIoTaskTest.class.getSimpleName());
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
        new RandomAccessFile(file, "rw").close();
        dtFile = new LogFile(0, Long.MAX_VALUE, file, fiberGroup, MockExecutors.ioExecutor(), null, 0, true);
        dtFile.syncOpen();
        groupConfig = new RaftGroupConfigEx(1, "1", "");
        groupConfig.ioRetryInterval = new int[]{1};
        groupConfig.fiberGroup = fiberGroup;
        groupConfig.blockIoExecutor = MockExecutors.ioExecutor();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterEach
    public void clean() {
        if (file != null && file.exists()) {
            dtFile.destroy();
            file.delete();
        }
    }

    @Test
    public void testRW() throws Exception {
        doInFiber(new FiberFrame<>() {
            ByteBuffer buf = ByteBuffer.allocate(1024);

            @Override
            public FrameCallResult execute(Void input) {
                AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile);
                return t.write(buf, 0).await(1000, this::resume1);
            }

            private FrameCallResult resume1(Void unused) {
                buf = ByteBuffer.allocate(768);
                AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile);
                return t.read(buf, 0).await(1000, this::resume2);
            }

            private FrameCallResult resume2(Void unused) {
                buf.clear();
                AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile);
                return t.read(buf, 512).await(1000, this::resume3);
            }

            private FrameCallResult resume3(Void unused) {
                throw new AssertionError();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                assertTrue(ex.getMessage().contains("read end of file"));
                return Fiber.frameReturn();
            }
        });
    }

    private void assertReadSuccess(Supplier<IoFailTask> supplier) throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                ByteBuffer buf = ByteBuffer.allocate(1);
                IoFailTask t = supplier.get();
                return t.read(buf, 0).await(1000, this::justReturn);
            }
        });
    }

    private void assertReadFail(Supplier<IoFailTask> supplier) throws Exception {
        doInFiber(new FiberFrame<>() {
            IoFailTask t;

            @Override
            public FrameCallResult execute(Void input) {
                ByteBuffer buf = ByteBuffer.allocate(1);
                t = supplier.get();
                return t.read(buf, 0).await(1000, this::resume);
            }

            private FrameCallResult resume(Void unused) {
                throw new AssertionError();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                assertSame(t.ex, DtUtil.rootCause(ex));
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testReadEx() throws Exception {

        ByteBuffer buf = ByteBuffer.allocate(1);

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                AsyncIoTask wt = new AsyncIoTask(fiberGroup, dtFile);
                return wt.write(buf, 0)
                        .await(1000, this::justReturn);
            }
        });

        // fail on first read
        assertReadSuccess(() -> new IoFailTask(1, true, null));

        // fail twice, so retry failed
        assertReadFail(() -> new IoFailTask(2, true, null));

        // fail, cancel indicator return true
        assertReadFail(() -> new IoFailTask(2, true, () -> true));

        // fail, cancel indicator turns true after first check
        AtomicInteger cancelIndicatorCount = new AtomicInteger();
        groupConfig.ioRetryInterval = new int[]{1, 1};
        assertReadFail(() -> new IoFailTask(3, true,
                () -> cancelIndicatorCount.getAndIncrement() == 1));

        // no retry
        assertReadFail(() -> new IoFailTask(2, false, null));
    }

    private class IoFailTask extends AsyncIoTask {
        private final int failCount;
        private int count;
        IOException ex = new IOException("mock error");

        public IoFailTask(int failCount, boolean retry, Supplier<Boolean> cancelIndicator) {
            super(groupConfig.fiberGroup, dtFile, retry ? groupConfig.ioRetryInterval : null,
                    cancelIndicator);
            this.failCount = failCount;
        }

        @Override
        protected void doExec(long pos) {
            if (count++ == failCount) {
                super.doExec(pos);
            } else {
                retry(ex);
            }
        }
    }

    @Test
    public void testAsyncOpen() throws Exception {
        file = new File(dir, "testAsyncOpen");
        new RandomAccessFile(file, "rw").close();
        dtFile = new LogFile(0, Long.MAX_VALUE, file, fiberGroup, MockExecutors.ioExecutor(), null, 0, true);

        doInFiber(new FiberFrame<>() {
            ByteBuffer buf = ByteBuffer.allocate(8);

            @Override
            public FrameCallResult execute(Void input) {
                assertFalse(dtFile.isRwChannelOpen());
                AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile);
                return t.write(buf, 0).await(1000, this::resume1);
            }

            private FrameCallResult resume1(Void unused) {
                assertTrue(dtFile.isRwChannelOpen());
                buf = ByteBuffer.allocate(8);
                AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile);
                return t.read(buf, 0).await(1000, this::resume2);
            }

            private FrameCallResult resume2(Void unused) {
                assertTrue(dtFile.isRwChannelOpen());
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testWriteEx() throws Exception {

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                ByteBuffer buf = ByteBuffer.allocate(1);
                // fail on first write
                IoFailTask t = new IoFailTask(1, true, () -> false);
                return t.write(buf, 0)
                        .await(1000, this::justReturn);
            }
        });

    }

    private static ByteBuffer dataBuf(int size, int value) {
        ByteBuffer buf = ByteBuffer.allocate(size);
        for (int i = 0; i < size; i++) {
            buf.put((byte) value);
        }
        buf.flip();
        return buf;
    }

    private void gatheringWriteAndVerify(Supplier<AsyncIoTask> supplier) throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                ByteBuffer[] bufs = {dataBuf(100, 1), dataBuf(200, 2), dataBuf(50, 3)};
                AsyncIoTask t = supplier.get();
                return t.write(bufs, 10).await(1000, v -> resumeRead(bufs));
            }

            private FrameCallResult resumeRead(ByteBuffer[] bufs) {
                for (ByteBuffer b : bufs) {
                    assertFalse(b.hasRemaining());
                }
                ByteBuffer buf = ByteBuffer.allocate(360);
                AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile);
                return t.read(buf, 0).await(1000, v -> resumeVerify(buf));
            }

            private FrameCallResult resumeVerify(ByteBuffer buf) {
                buf.flip();
                for (int i = 0; i < 360; i++) {
                    byte expect;
                    if (i < 10) {
                        expect = 0;
                    } else if (i < 110) {
                        expect = 1;
                    } else if (i < 310) {
                        expect = 2;
                    } else {
                        expect = 3;
                    }
                    assertEquals(expect, buf.get());
                }
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testGatheringWrite() throws Exception {
        gatheringWriteAndVerify(() -> new AsyncIoTask(fiberGroup, dtFile));
    }

    @Test
    public void testGatheringWriteDegrade() throws Exception {
        // hold the lock so the gathering write degrades to positional writes
        dtFile.gatheringWriteLock.lock();
        try {
            gatheringWriteAndVerify(() -> new AsyncIoTask(fiberGroup, dtFile));
        } finally {
            dtFile.gatheringWriteLock.unlock();
        }
    }

    @Test
    public void testGatheringWriteRetry() throws Exception {
        // fail once, verify buffers are rewound on retry
        gatheringWriteAndVerify(() -> new IoFailTask(1, true, () -> false));
    }

    private void writeAndForceAndVerify(Supplier<AsyncIoTask> supplier) throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                ByteBuffer buf = dataBuf(100, 7);
                AsyncIoTask t = supplier.get();
                return t.writeAndForce(buf, 10).await(1000, v -> resumeRead());
            }

            private FrameCallResult resumeRead() {
                ByteBuffer buf = ByteBuffer.allocate(110);
                AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile);
                return t.read(buf, 0).await(1000, v -> resumeVerify(buf));
            }

            private FrameCallResult resumeVerify(ByteBuffer buf) {
                buf.flip();
                for (int i = 0; i < 110; i++) {
                    assertEquals(i < 10 ? 0 : 7, buf.get());
                }
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testWriteAndForce() throws Exception {
        writeAndForceAndVerify(() -> new AsyncIoTask(fiberGroup, dtFile));
    }

    @Test
    public void testWriteAndForceRetry() throws Exception {
        // fail once, verify the buffer is rewound on retry
        writeAndForceAndVerify(() -> new IoFailTask(1, true, () -> false));
    }

    @Test
    public void testOpenRetry() throws Exception {
        file = new File(dir, "testOpenRetry");
        new RandomAccessFile(file, "rw").close();
        AtomicBoolean openFail = new AtomicBoolean(true);
        dtFile = new LogFile(0, Long.MAX_VALUE, file, fiberGroup, MockExecutors.ioExecutor(), null, 0, true) {
            @Override
            protected FileChannel doSyncOpen() throws IOException {
                if (openFail.compareAndSet(true, false)) {
                    throw new IOException("mock open error");
                }
                return super.doSyncOpen();
            }
        };

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                assertFalse(dtFile.isRwChannelOpen());
                ByteBuffer buf = dataBuf(8, 1);
                AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile,
                        groupConfig.ioRetryInterval, () -> false);
                return t.write(buf, 0).await(1000, this::resume);
            }

            private FrameCallResult resume(Void unused) {
                assertTrue(dtFile.isRwChannelOpen());
                assertFalse(openFail.get());
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testRetryReopensChannel() throws Exception {        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                ByteBuffer buf = dataBuf(8, 3);
                AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile,
                        groupConfig.ioRetryInterval, null) {
                    private boolean failed;

                    @Override
                    protected void doExec(long pos) {
                        if (!failed) {
                            failed = true;
                            // simulate an idle close racing the io failure
                            dtFile.doClose();
                            retry(new IOException("mock error"));
                        } else {
                            super.doExec(pos);
                        }
                    }
                };
                return t.write(buf, 0).await(1000, this::resumeRead);
            }

            private FrameCallResult resumeRead(Void unused) {
                assertTrue(dtFile.isRwChannelOpen());
                ByteBuffer buf = ByteBuffer.allocate(8);
                AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile);
                return t.read(buf, 0).await(1000, v -> resumeVerify(buf));
            }

            private FrameCallResult resumeVerify(ByteBuffer buf) {
                buf.flip();
                while (buf.hasRemaining()) {
                    assertEquals(3, buf.get());
                }
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testDestroyedFileFailFast() throws Exception {
        // io on a destroyed file fails fast instead of retrying forever
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                dtFile.destroy();
                ByteBuffer buf = dataBuf(8, 1);
                AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile,
                        groupConfig.ioRetryInterval, () -> false);
                return t.write(buf, 0).await(1000, this::resume);
            }

            private FrameCallResult resume(Void unused) {
                throw new AssertionError();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                assertTrue(DtUtil.rootCause(ex).getMessage().contains("destroyed"));
                return Fiber.frameReturn();
            }
        });
    }
}
