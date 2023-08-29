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

import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class IdxFileQueueTest {

    private IdxFileQueue idxFileQueue;
    private RaftStatusImpl raftStatus;

    @BeforeEach
    public void setup() {
        File dir = TestDir.createTestDir(IdxFileQueueTest.class.getSimpleName());
        RaftGroupConfigEx c = new RaftGroupConfigEx(1, "1", "1");
        c.setRaftExecutor(MockExecutors.raftExecutor());
        c.setStopIndicator(() -> false);
        c.setTs(new Timestamp());
        raftStatus = new RaftStatusImpl();
        raftStatus.setStatusFile(new StatusFile(new File(dir, "test.status")));
        raftStatus.getStatusFile().init();
        c.setRaftStatus(raftStatus);
        c.setIoExecutor(MockExecutors.ioExecutor());
        idxFileQueue = new IdxFileQueue(dir, new StatusManager(MockExecutors.ioExecutor()), c, 8, 4);
    }

    @AfterEach
    public void tearDown() {
        idxFileQueue.close();
    }

    @Test
    public void testPut1() throws Exception {
        idxFileQueue.init();
        idxFileQueue.initRestorePos();
        for (int i = 1; i <= 10; i++) {
            idxFileQueue.put(i, i * 100);
        }
        assertEquals(10, idxFileQueue.tailCache.size());
    }

    @Test
    public void testPut2() throws Exception {
        idxFileQueue.init();
        idxFileQueue.initRestorePos();
        for (int i = 1; i <= 30; i++) {
            raftStatus.setCommitIndex(i - 1);
            idxFileQueue.put(i, i * 100);
        }
        assertTrue(idxFileQueue.tailCache.size() <= 5);
    }
}
