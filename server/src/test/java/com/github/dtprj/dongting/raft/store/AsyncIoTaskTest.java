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
import com.github.dtprj.dongting.raft.RaftException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
public class AsyncIoTaskTest {
    private static File dir;
    private File file;

    @BeforeAll
    public static void setup() {
        dir = TestDir.createTestDir(AsyncIoTaskTest.class.getSimpleName());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterEach
    public void clean() {
        if (file != null && file.exists()) {
            file.delete();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterAll
    public static void cleanDir() {
        if (dir.exists()) {
            dir.delete();
        }
    }

    @Test
    public void testWrite() throws Exception {
        file = new File(dir, "testFile");
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(file.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        ByteBuffer buf = ByteBuffer.allocate(1024);
        AsyncIoTask t = new AsyncIoTask(channel, null, null);
        t.write(true, false, buf, 0).get();
    }

    @Test
    public void testRead() throws Exception {
        testWrite();
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(file.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        ByteBuffer buf = ByteBuffer.allocate(768);
        AsyncIoTask t = new AsyncIoTask(channel, null, null);
        t.read(buf, 0).get();

        buf.clear();
        try {
            t.read(buf, 512).get();
            fail();
        } catch (Exception e) {
            assertEquals(RaftException.class, DtUtil.rootCause(e).getClass());
        }
    }

    @Test
    public void testRetry() throws Exception {
        file = new File(dir, "testFile");
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(file.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        ByteBuffer writeBuffer = ByteBuffer.allocate(16);
        for (int i = 0; i < 16; i++) {
            writeBuffer.put((byte) i);
        }
        writeBuffer.clear();
        AsyncIoTask writeTask = new AsyncIoTask(channel, () -> false, () -> false);
        writeTask.write(false, false, writeBuffer, 0).get();

        AsyncIoTask readTask = new AsyncIoTask(channel, () -> false, () -> false);
        ByteBuffer readBuffer = ByteBuffer.allocate(32);
        try {
            readTask.read(readBuffer, 0).get();
            fail();
        } catch (Exception e) {
            assertEquals(RaftException.class, DtUtil.rootCause(e).getClass());
        }

        writeBuffer.clear();
        for (int i = 16; i < 32; i++) {
            writeBuffer.put((byte) i);
        }
        writeBuffer.clear();
        writeTask.write(false, false, writeBuffer, 16).get();

        readTask.retry().get();
        for (int i = 0; i < 32; i++) {
            assertEquals(i, readBuffer.get(i));
        }
    }
}
