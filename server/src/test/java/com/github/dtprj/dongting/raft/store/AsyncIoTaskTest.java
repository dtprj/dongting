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

import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.raft.RaftException;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.github.dtprj.dongting.raft.test.FiberUtil.toJdkFuture;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class AsyncIoTaskTest extends BaseFiberTest {
    private static File dir;
    private File file;
    private AsynchronousFileChannel channel;

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
        channel = AsynchronousFileChannel.open(file.toPath(), s, MockExecutors.ioExecutor());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterEach
    public void clean() throws Exception {
        if (file != null && file.exists()) {
            channel.close();
            file.delete();
        }
    }

    @Test
    public void testRW() throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        AsyncIoTask t = new AsyncIoTask(fiberGroup, channel);
        FiberFuture<Void> f = t.write(buf, 0);
        toJdkFuture(f).get(1, TimeUnit.SECONDS);

        buf = ByteBuffer.allocate(768);
        t = new AsyncIoTask(fiberGroup, channel);
        toJdkFuture(t.read(buf, 0)).get(1, TimeUnit.SECONDS);

        buf.clear();
        try {
            t = new AsyncIoTask(fiberGroup, channel);
            toJdkFuture(t.read(buf, 512)).get(1, TimeUnit.SECONDS);
            fail();
        } catch (Exception e) {
            assertEquals(RaftException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testReadEx() throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(1);

        AsyncIoTask wt = new AsyncIoTask(fiberGroup, channel);
        toJdkFuture(wt.writeAndFlush(buf, 0, false)).get(1, TimeUnit.SECONDS);

        // fail on first read
        buf.clear();
        ReadFailTask t = new ReadFailTask(1, new long[]{1}, false, null);
        toJdkFuture(t.read(buf, 0)).get(1, TimeUnit.SECONDS);

        // fail twice, so retry failed
        buf.clear();
        t = new ReadFailTask(2, new long[]{1}, false, null);
        try {
            toJdkFuture(t.read(buf, 0)).get(1, TimeUnit.SECONDS);
            fail();
        } catch (Exception e) {
            assertSame(t.ex, e.getCause());
        }

        // fail twice but retry forever
        buf.clear();
        t = new ReadFailTask(2, new long[]{1}, true, () -> false);
        toJdkFuture(t.read(buf, 0)).get(1, TimeUnit.SECONDS);

        // fail, cancel indicator return true
        buf.clear();
        t = new ReadFailTask(2, new long[]{1}, true, () -> true);
        try {
            toJdkFuture(t.read(buf, 0)).get(1, TimeUnit.SECONDS);
            fail();
        } catch (Exception e) {
            assertSame(t.ex, e.getCause());
        }

        // fail, cancel indicator return true
        buf.clear();
        AtomicInteger cancelIndicatorCount = new AtomicInteger();
        t = new ReadFailTask(2, new long[]{1}, true,
                () -> cancelIndicatorCount.getAndIncrement() == 1);
        try {
            toJdkFuture(t.read(buf, 0)).get(1, TimeUnit.SECONDS);
            fail();
        } catch (Exception e) {
            assertSame(t.ex, e.getCause());
        }

        // no retry
        buf.clear();
        t = new ReadFailTask(2, null, false, null);
        try {
            toJdkFuture(t.read(buf, 0)).get(1, TimeUnit.SECONDS);
            fail();
        } catch (Exception e) {
            assertSame(t.ex, e.getCause());
        }
    }

    private class ReadFailTask extends AsyncIoTask {
        private final int failCount;
        private int count;
        IOException ex = new IOException("mock error");

        public ReadFailTask(int failCount, long[] retryInterval,
                            boolean retryForever, Supplier<Boolean> cancelIndicator) {
            super(fiberGroup, channel, retryInterval, retryForever, cancelIndicator);
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
        ByteBuffer buf = ByteBuffer.allocate(1);

        // fail on first read
        FlushFailTask t = new FlushFailTask(1, new long[]{1}, false, () -> false);
        toJdkFuture(t.writeAndFlush(buf, 0, false)).get(1, TimeUnit.SECONDS);
    }

    private class FlushFailTask extends AsyncIoTask {
        private final int failCount;
        private int count;
        IOException ex = new IOException("mock error");

        public FlushFailTask(int failCount, long[] retryInterval, boolean retryForever,
                             Supplier<Boolean> cancelIndicator) {
            super(fiberGroup, channel, retryInterval, retryForever, cancelIndicator);
            this.failCount = failCount;
        }

        @Override
        protected void doFlush() throws IOException {
            if (count++ == failCount) {
                super.doFlush();
            } else {
                throw ex;
            }
        }
    }
}
