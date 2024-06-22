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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.function.Predicate;

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
        c.setBlockIoExecutor(MockExecutors.ioExecutor());
        RaftStatusImpl raftStatus = new RaftStatusImpl(dispatcher.getTs());
        raftStatus.setTailCache(new TailCache(c, raftStatus));
        c.setRaftStatus(raftStatus);
        c.setFiberGroup(fiberGroup);
        fileQueue = new MockFileQueue(dir, c, 1024);
    }

    private static class MockFileQueue extends FileQueue {
        public MockFileQueue(File dir, RaftGroupConfigEx groupConfig, long fileSize) {
            super(dir, groupConfig, fileSize, false);
        }
    }

    @Test
    public void testInit1() throws Exception {
        fileQueue.initQueue();
        assertEquals(0, fileQueue.queue.size());
    }

    @Test
    public void testInit2() throws Exception {
        File f1 = new File(fileQueue.dir, "00000000000000000000");
        RandomAccessFile raf1 = new RandomAccessFile(f1, "rw");
        raf1.setLength(1023);
        assertThrows(RaftException.class, () -> fileQueue.initQueue());
        assertEquals(0, fileQueue.queue.size());
        raf1.close();
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
                fileQueue.setInitialized(true);
                return Fiber.call(fileQueue.ensureWritePosReady(2048), this::resume);
            }

            private FrameCallResult resume(Void unused) {
                assertNotNull(fileQueue.getLogFile(0));
                assertNotNull(fileQueue.getLogFile(1024));
                assertNotNull(fileQueue.getLogFile(2048));

                Predicate<LogFile> p = lf -> {
                    String n = lf.getFile().getName();
                    return n.endsWith("0000") || n.endsWith("1024");
                };
                return Fiber.call(fileQueue.deleteByPredicate(p), this::resume2);
            }

            private FrameCallResult resume2(Void unused) {
                assertNull(fileQueue.getLogFile(0));
                assertNull(fileQueue.getLogFile(1024));
                assertNotNull(fileQueue.getLogFile(2048));
                return Fiber.frameReturn();
            }
        });
    }

}
