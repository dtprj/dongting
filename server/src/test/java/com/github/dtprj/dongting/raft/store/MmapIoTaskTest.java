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

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class MmapIoTaskTest extends BaseFiberTest {
    private static File dir;
    private File file;
    private DtFile dtFile;

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
        dtFile = new DtFile(file, fiberGroup, s, MockExecutors.ioExecutor());
        dtFile.syncOpenMmap();
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
    public void testRead() throws Exception {
        // write data via AsyncIoTask first
        doInFiber(new FiberFrame<>() {
            final ByteBuffer writeBuf = ByteBuffer.allocate(100);

            @Override
            public FrameCallResult execute(Void input) {
                writeBuf.putInt(12345);
                writeBuf.flip();
                AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile);
                return t.write(writeBuf, 0).await(1000, this::afterWrite);
            }

            private FrameCallResult afterWrite(Void unused) {
                ByteBuffer readBuf = ByteBuffer.allocate(4);
                MmapIoTask t = new MmapIoTask(fiberGroup, dtFile);
                return t.run(new SingleBufferCallback(readBuf, 0)).await(1000, v -> {
                    readBuf.flip();
                    assertEquals(12345, readBuf.getInt());
                    return Fiber.frameReturn();
                });
            }
        });
    }

    @Test
    public void testReadFail() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                MmapIoTask t = new MmapIoTask(fiberGroup, dtFile);
                return t.run(mmapBuffer -> {
                    throw MOCK_EX;
                }).await(1000, this::resume);
            }

            private FrameCallResult resume(Void unused) {
                throw new AssertionError("should not reach here");
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                assertSame(MOCK_EX, DtUtil.rootCause(ex));
                return Fiber.frameReturn();
            }
        });
    }

    private static final IOException MOCK_EX = new IOException("mock error");

    @Test
    public void testAsyncOpen() throws Exception {
        dtFile.destroy();
        file.delete();

        file = new File(dir, "testAsyncOpen");
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(4096);
        raf.close();
        Set<OpenOption> s = Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        dtFile = new DtFile(file, fiberGroup, s, MockExecutors.ioExecutor());

        doInFiber(new FiberFrame<>() {
            final ByteBuffer buf = ByteBuffer.allocate(8);

            @Override
            public FrameCallResult execute(Void input) {
                assertFalse(dtFile.isMmapOpen());
                MmapIoTask t = new MmapIoTask(fiberGroup, dtFile);
                return t.run(new SingleBufferCallback(buf, 0)).await(1000, this::resume1);
            }

            private FrameCallResult resume1(Void unused) {
                assertTrue(dtFile.isMmapOpen());
                return Fiber.frameReturn();
            }
        });
    }
}
