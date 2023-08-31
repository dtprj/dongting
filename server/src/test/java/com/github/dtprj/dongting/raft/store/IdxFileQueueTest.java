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

import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static com.github.dtprj.dongting.raft.store.IdxFileQueue.IDX_FILE_PERSIST_INDEX_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class IdxFileQueueTest {

    private IdxFileQueue idxFileQueue;
    private RaftStatusImpl raftStatus;
    private StatusManager statusManager;
    private File dir;

    @BeforeEach
    public void setup() {
        dir = TestDir.createTestDir(IdxFileQueueTest.class.getSimpleName());
        idxFileQueue = createFileQueue();
    }

    private IdxFileQueue createFileQueue() {
        RaftGroupConfigEx c = new RaftGroupConfigEx(1, "1", "1");
        c.setRaftExecutor(MockExecutors.raftExecutor());
        c.setStopIndicator(() -> false);
        c.setTs(new Timestamp());
        raftStatus = new RaftStatusImpl();
        statusManager = new StatusManager(MockExecutors.ioExecutor(), raftStatus);
        statusManager.initStatusFileChannel(dir.getPath(), "test.status");
        c.setRaftStatus(raftStatus);
        c.setIoExecutor(MockExecutors.ioExecutor());
        return new IdxFileQueue(dir, new StatusManager(MockExecutors.ioExecutor(), raftStatus), c, 8, 4);
    }

    @AfterEach
    public void tearDown() {
        idxFileQueue.close();
        statusManager.close();
    }

    @Test
    public void testPut1() throws Exception {
        idxFileQueue.init();
        idxFileQueue.initRestorePos();
        for (int i = 1; i <= 10; i++) {
            idxFileQueue.put(i, i * 100, false);
        }
        assertEquals(10, idxFileQueue.tailCache.size());
    }

    @Test
    public void testPut2() throws Exception {
        idxFileQueue.init();
        idxFileQueue.initRestorePos();
        for (int i = 1; i <= 30; i++) {
            raftStatus.setCommitIndex(i - 1);
            idxFileQueue.put(i, i * 100, false);
        }
        assertTrue(idxFileQueue.tailCache.size() <= 5);
    }

    @Test
    public void testPutError1() throws Exception {
        idxFileQueue.init();
        idxFileQueue.initRestorePos();
        assertThrows(RaftException.class, () -> idxFileQueue.put(10, 1000, false));
        assertThrows(RaftException.class, () -> idxFileQueue.put(0, 1000, false));
    }

    @Test
    public void testPutError2() throws Exception {
        idxFileQueue.init();
        idxFileQueue.initRestorePos();
        for (int i = 1; i <= 10; i++) {
            raftStatus.setCommitIndex(i - 1);
            idxFileQueue.put(i, i * 100, false);
        }
        assertThrows(RaftException.class, () -> idxFileQueue.put(5, 500, false));
    }

    @Test
    public void testPutTruncate() throws Exception {
        idxFileQueue.init();
        idxFileQueue.initRestorePos();
        for (int i = 1; i <= 10; i++) {
            idxFileQueue.put(i, i * 100, false);
        }
        idxFileQueue.put(5, 5000, false);
        idxFileQueue.put(6, 6000, false);
        assertEquals(6, idxFileQueue.tailCache.size());
        assertEquals(400, idxFileQueue.syncLoadLogPos(4));
        assertEquals(5000, idxFileQueue.syncLoadLogPos(5));
        assertEquals(6000, idxFileQueue.syncLoadLogPos(6));
    }

    @Test
    public void testTruncate() throws Exception {
        idxFileQueue.init();
        idxFileQueue.initRestorePos();
        for (int i = 1; i <= 10; i++) {
            idxFileQueue.put(i, i * 100, false);
        }
        idxFileQueue.truncateTail(5);
        assertEquals(4, idxFileQueue.tailCache.size());
        assertEquals(400, idxFileQueue.syncLoadLogPos(4));
        assertThrows(RaftException.class, () -> idxFileQueue.syncLoadLogPos(5));

        raftStatus.setCommitIndex(3);
        assertThrows(RaftException.class, () -> idxFileQueue.truncateTail(3));
        assertThrows(RaftException.class, () -> idxFileQueue.truncateTail(5));
        idxFileQueue.truncateTail(4);
    }

    @Test
    public void testSyncLoad() throws Exception {
        idxFileQueue.init();
        idxFileQueue.initRestorePos();
        for (int i = 1; i <= 30; i++) {
            raftStatus.setCommitIndex(i - 1);
            idxFileQueue.put(i, i * 100, false);
        }
        for (int i = 1; i <= 30; i++) {
            assertEquals(i * 100, idxFileQueue.syncLoadLogPos(i));
        }
        idxFileQueue.submitDeleteTask(10);
        TestUtil.waitUtil(() -> idxFileQueue.queueStartPosition == 8 << 3);
        assertThrows(RaftException.class, () -> idxFileQueue.syncLoadLogPos(1));
        assertThrows(RaftException.class, () -> idxFileQueue.syncLoadLogPos(31));
    }

    @Test
    public void testInit1() throws Exception {
        idxFileQueue.init();
        idxFileQueue.initRestorePos();
        for (int i = 1; i <= 30; i++) {
            raftStatus.setCommitIndex(i - 1);
            idxFileQueue.put(i, i * 100, false);
        }

        idxFileQueue.close();
        statusManager.close();

        idxFileQueue = createFileQueue();
        idxFileQueue.init();
        Pair<Long, Long> p = idxFileQueue.initRestorePos();
        assertEquals(29, p.getLeft());
        assertEquals(2900, p.getRight());
        assertEquals(30, idxFileQueue.getNextIndex());
        assertEquals(30, idxFileQueue.getNextPersistIndex());
        assertEquals(0, idxFileQueue.queueStartPosition);
    }

    @Test
    public void testInit2() throws Exception {
        idxFileQueue.init();
        idxFileQueue.initRestorePos();
        for (int i = 1; i <= 30; i++) {
            raftStatus.setCommitIndex(i - 1);
            idxFileQueue.put(i, i * 100, false);
        }
        idxFileQueue.submitDeleteTask(10);
        TestUtil.waitUtil(() -> idxFileQueue.queueStartPosition == 8 << 3);

        idxFileQueue.close();
        raftStatus.getExtraPersistProps().setProperty(IDX_FILE_PERSIST_INDEX_KEY, "2");
        statusManager.persistSync();
        statusManager.close();

        idxFileQueue = createFileQueue();
        idxFileQueue.init();
        Pair<Long, Long> p = idxFileQueue.initRestorePos();
        assertEquals(8, p.getLeft());
        assertEquals(800, p.getRight());
        assertEquals(9, idxFileQueue.getNextIndex());
        assertEquals(9, idxFileQueue.getNextPersistIndex());
        assertEquals(8 << 3, idxFileQueue.queueStartPosition);

        // mock recover
        for (int i = 9; i <= 30; i++) {
            idxFileQueue.put(i, i * 100, true);
        }
    }

}
