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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.test.TestDir;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class FileQueueTest extends BaseFiberTest {

    private MockFileQueue fileQueue;

    @BeforeEach
    public void setup() {
        File dir = TestDir.createTestDir(FileQueueTest.class.getSimpleName());
        RaftGroupConfigEx c = new RaftGroupConfigEx(1, "1", "1");
        c.blockIoExecutor = MockExecutors.ioExecutor();
        RaftStatusImpl raftStatus = new RaftStatusImpl(1, dispatcher.ts);
        raftStatus.tailCache = new TailCache(c, raftStatus);
        c.raftStatus = raftStatus;
        c.fiberGroup = fiberGroup;
        fileQueue = new MockFileQueue(dir, c, 1024);
    }

    @AfterEach
    public void cleanup() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return fileQueue.stopFileQueue().await(this::justReturn);
            }
        });
    }

    private static class MockFileQueue extends FileQueue {
        public MockFileQueue(File dir, RaftGroupConfigEx groupConfig, long fileSize) {
            super(dir, groupConfig, fileSize, false);
        }
    }

    @Test
    public void testInit1() {
        fileQueue.initQueue();
        assertEquals(0, fileQueue.queue.size());
    }

    @Test
    public void testInit2() throws Exception {
        // a crash during pre-allocation may leave the last file with length 0,
        // it is re-extended instead of failing
        File f1 = new File(fileQueue.dir, "00000000000000000000");
        RandomAccessFile raf1 = new RandomAccessFile(f1, "rw");
        raf1.setLength(0);
        raf1.close();
        fileQueue.initQueue();
        assertEquals(1, fileQueue.queue.size());
        assertEquals(1024, f1.length());
        assertEquals(0, fileQueue.queueStartPosition);
        assertEquals(1024, fileQueue.queueEndPosition);
    }

    @Test
    public void testInit2b() throws Exception {
        // a non-last file with wrong size indicates real corruption
        File f1 = new File(fileQueue.dir, "00000000000000000000");
        File f2 = new File(fileQueue.dir, "00000000000000001024");
        RandomAccessFile raf1 = new RandomAccessFile(f1, "rw");
        RandomAccessFile raf2 = new RandomAccessFile(f2, "rw");
        raf1.setLength(1023);
        raf2.setLength(1024);
        raf1.close();
        raf2.close();
        assertThrows(RaftException.class, () -> fileQueue.initQueue());
        assertEquals(1023, f1.length());
    }

    @Test
    public void testInit2c() throws Exception {
        // last file with a size other than 0 and fileSize is not possible
        // on the normal path, treat it as corruption
        File f1 = new File(fileQueue.dir, "00000000000000000000");
        RandomAccessFile raf1 = new RandomAccessFile(f1, "rw");
        raf1.setLength(1023);
        raf1.close();
        assertThrows(RaftException.class, () -> fileQueue.initQueue());
        assertEquals(1023, f1.length());
    }

    @Test
    public void testInit3() throws Exception {
        File f1 = new File(fileQueue.dir, "00000000000000000000");
        RandomAccessFile raf1 = new RandomAccessFile(f1, "rw");
        raf1.setLength(1024);
        fileQueue.initQueue();
        assertEquals(1, fileQueue.queue.size());
        assertEquals(0, fileQueue.queueStartPosition);
        assertEquals(1024, fileQueue.queueEndPosition);
        raf1.close();
    }

    @Test
    public void testInit4() throws Exception {
        File f1 = new File(fileQueue.dir, "00000000000000001024");
        File f2 = new File(fileQueue.dir, "00000000000000002048");
        RandomAccessFile raf1 = new RandomAccessFile(f1, "rw");
        RandomAccessFile raf2 = new RandomAccessFile(f2, "rw");
        raf1.setLength(1024);
        raf2.setLength(1024);
        fileQueue.initQueue();
        assertEquals(2, fileQueue.queue.size());
        assertEquals(1024, fileQueue.queueStartPosition);
        assertEquals(3072, fileQueue.queueEndPosition);
        raf1.close();
        raf2.close();
    }

    @Test
    public void testInit5() throws Exception {
        File f1 = new File(fileQueue.dir, "00000000000000001023");
        RandomAccessFile raf1 = new RandomAccessFile(f1, "rw");
        raf1.setLength(1024);
        assertThrows(RaftException.class, () -> fileQueue.initQueue());
        raf1.close();
    }

    @Test
    public void testInit6() throws Exception {
        File f1 = new File(fileQueue.dir, "00000000000000001024");
        File f2 = new File(fileQueue.dir, "00000000000000004096");
        RandomAccessFile raf1 = new RandomAccessFile(f1, "rw");
        RandomAccessFile raf2 = new RandomAccessFile(f2, "rw");
        raf1.setLength(1024);
        raf2.setLength(1024);
        assertThrows(RaftException.class, () -> fileQueue.initQueue());
        raf1.close();
        raf2.close();
    }

    @Test
    public void testEnsureWritePosReady() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                fileQueue.startQueueAllocFiber();
                return Fiber.call(fileQueue.ensureWritePosReady(0), this::resume);
            }

            private FrameCallResult resume(Void unused) {
                assertEquals(1, fileQueue.queue.size());
                return Fiber.call(fileQueue.ensureWritePosReady(1024), this::resume2);
            }

            private FrameCallResult resume2(Void unused) {
                assertEquals(2, fileQueue.queue.size());
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    public void testDelete() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                fileQueue.initialized = true;
                fileQueue.startQueueAllocFiber();
                return Fiber.call(fileQueue.ensureWritePosReady(2048), this::resume);
            }

            private FrameCallResult resume(Void unused) {
                assertNotNull(fileQueue.getLogFile(0));
                assertNotNull(fileQueue.getLogFile(1024));
                assertNotNull(fileQueue.getLogFile(2048));
                // delete 1 file
                return Fiber.call(fileQueue.deleteFirstFile(), this::resume2);
            }

            private FrameCallResult resume2(Void unused) {
                assertNull(fileQueue.getLogFile(0));
                assertNotNull(fileQueue.getLogFile(1024));
                assertNotNull(fileQueue.getLogFile(2048));
                LogFile logFile = fileQueue.getLogFile(1024);
                logFile.incWriters();
                Fiber f = new Fiber("f", getFiberGroup(), new FiberFrame<>() {
                    @Override
                    public FrameCallResult execute(Void input) {
                        logFile.decWriters();
                        return Fiber.frameReturn();
                    }
                });
                f.start();
                return Fiber.call(fileQueue.deleteFirstFile(), this::resume3);
            }

            private FrameCallResult resume3(Void unused) {
                assertNull(fileQueue.getLogFile(1024));
                assertNotNull(fileQueue.getLogFile(2048));
                return Fiber.frameReturn();
            }
        });
    }

}
