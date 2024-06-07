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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class AsyncIoTaskTest extends BaseFiberTest {
    private static File dir;
    private File file;
    private DtFile dtFile;
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
        Set<OpenOption> s = new HashSet<>();
        s.add(StandardOpenOption.CREATE);
        s.add(StandardOpenOption.WRITE);
        s.add(StandardOpenOption.READ);
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(file.toPath(), s, fiberGroup.getExecutor());
        dtFile = new DtFile(file, channel, fiberGroup);
        groupConfig = new RaftGroupConfigEx(0, "1", "");
        groupConfig.setIoExecutor(MockExecutors.ioExecutor());
        groupConfig.setIoRetryInterval(new long[]{1});
        groupConfig.setFiberGroup(fiberGroup);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterEach
    public void clean() throws Exception {
        if (file != null && file.exists()) {
            dtFile.getChannel().close();
            file.delete();
        }
    }

    @Test
    public void testRW() throws Exception {
        doInFiber(new FiberFrame<>() {
            ByteBuffer buf = ByteBuffer.allocate(1024);

            @Override
            public FrameCallResult execute(Void input) {
                AsyncIoTask t = new AsyncIoTask(groupConfig, dtFile);
                return t.write(buf, 0).await(1000, this::resume1);
            }

            private FrameCallResult resume1(Void unused) {
                buf = ByteBuffer.allocate(768);
                AsyncIoTask t = new AsyncIoTask(groupConfig, dtFile);
                return t.read(buf, 0).await(1000, this::resume2);
            }

            private FrameCallResult resume2(Void unused) {
                buf.clear();
                AsyncIoTask t = new AsyncIoTask(groupConfig, dtFile);
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

    private void assertReadSuccess(Supplier<ReadFailTask> supplier) throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                ByteBuffer buf = ByteBuffer.allocate(1);
                ReadFailTask t = supplier.get();
                return t.read(buf, 0).await(1000, this::justReturn);
            }
        });
    }

    private void assertReadFail(Supplier<ReadFailTask> supplier) throws Exception {
        doInFiber(new FiberFrame<>() {
            ReadFailTask t;

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
                AsyncIoTask wt = new AsyncIoTask(groupConfig, dtFile);
                return wt.writeAndForce(buf, 0, false)
                        .await(1000, this::justReturn);
            }
        });

        // fail on first read
        assertReadSuccess(() -> new ReadFailTask(1, true, false, null));

        // fail twice, so retry failed
        assertReadFail(() -> new ReadFailTask(2, true, false, null));

        // fail twice but retry forever
        assertReadSuccess(() -> new ReadFailTask(2, true, true, () -> false));

        // fail, cancel indicator return true
        assertReadFail(() -> new ReadFailTask(2, true, true, () -> true));

        // fail, cancel indicator return true
        AtomicInteger cancelIndicatorCount = new AtomicInteger();
        assertReadFail(() -> new ReadFailTask(2, true, true,
                () -> cancelIndicatorCount.getAndIncrement() == 1));

        // no retry
        assertReadFail(() -> new ReadFailTask(2, false, false, null));
    }

    private class ReadFailTask extends AsyncIoTask {
        private final int failCount;
        private int count;
        IOException ex = new IOException("mock error");

        public ReadFailTask(int failCount, boolean retry,
                            boolean retryForever, Supplier<Boolean> cancelIndicator) {
            super(groupConfig, dtFile, retry, retryForever, cancelIndicator);
            this.failCount = failCount;
        }

        @Override
        protected void exec(long filePos) {
            if (count++ == failCount) {
                super.exec(filePos);
            } else {
                failed(ex, null);
            }
        }
    }

    @Test
    public void testWriteEx() throws Exception {

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                ByteBuffer buf = ByteBuffer.allocate(1);
                // fail on first write
                FlushFailTask t = new FlushFailTask(1, true, false, () -> false);
                return t.writeAndForce(buf, 0, false)
                        .await(1000, this::justReturn);
            }
        });

    }

    private class FlushFailTask extends AsyncIoTask {
        private final int failCount;
        private int count;
        IOException ex = new IOException("mock error");

        public FlushFailTask(int failCount, boolean retry, boolean retryForever,
                             Supplier<Boolean> cancelIndicator) {
            super(groupConfig, dtFile, retry, retryForever, cancelIndicator);
            this.failCount = failCount;
        }

        @Override
        protected void doForce() throws IOException {
            if (count++ == failCount) {
                super.doForce();
            } else {
                throw ex;
            }
        }
    }
}
