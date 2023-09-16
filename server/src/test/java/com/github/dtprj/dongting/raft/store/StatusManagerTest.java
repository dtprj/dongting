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

import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.StoppedException;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author huangli
 */
public class StatusManagerTest {

    private StatusManager statusManager;
    private RaftStatusImpl raftStatus;
    private File dir;
    private CompletableFuture<Void> mockPersistResult;

    @BeforeEach
    public void setup() {
        dir = TestDir.createTestDir(StatusManagerTest.class.getSimpleName());
        raftStatus = new RaftStatusImpl();
        statusManager = new StatusManager(MockExecutors.ioExecutor(), raftStatus) {
            private boolean mockFail = false;
            @Override
            protected CompletableFuture<Void> persist(boolean flush) {
                if (mockPersistResult != null && !mockFail) {
                    mockFail = true;
                    return mockPersistResult;
                }
                return super.persist(flush);
            }
        };
        statusManager.initStatusFileChannel(dir.getParent(), "status");
        StatusManager.SYNC_FAIL_RETRY_INTERVAL = 1;
    }

    @AfterEach
    public void teardown() {
        mockPersistResult = null;
        statusManager.close();
        StatusManager.SYNC_FAIL_RETRY_INTERVAL = 1000;
    }

    private void check() {
        RaftStatusImpl s2 = new RaftStatusImpl();
        StatusManager m2 = new StatusManager(MockExecutors.ioExecutor(), s2);
        m2.initStatusFileChannel(dir.getParent(), "status");
        assertEquals(raftStatus.getCommitIndex(), s2.getCommitIndex());
        assertEquals(raftStatus.getVotedFor(), s2.getVotedFor());
        assertEquals(raftStatus.getCurrentTerm(), s2.getCurrentTerm());
        assertEquals(statusManager.getProperties().getProperty("k1"), m2.getProperties().getProperty("k1"));
        m2.close();
    }

    @Test
    public void testPersistSyncAndInit() {
        initData();
        statusManager.persistSync();
        statusManager.close();
        check();
    }

    private void initData() {
        raftStatus.setCommitIndex(100);
        raftStatus.setVotedFor(200);
        raftStatus.setCurrentTerm(300);
        statusManager.getProperties().setProperty("k1", "v1");
    }

    @Test
    public void testPersistAsync() throws Exception {
        CompletableFuture<Void> f = null;
        for (int i = 0; i < 5; i++) {
            raftStatus.setCommitIndex(100 + i);
            raftStatus.setVotedFor(200 + i);
            raftStatus.setCurrentTerm(300 + i);
            statusManager.getProperties().setProperty("k1", "v1" + i);
            f = statusManager.persistAsync();
        }
        f.get();
        statusManager.close();

        check();
    }

    @Test
    public void testMixPersist() {
        initData();
        statusManager.persistAsync();

        raftStatus.setCommitIndex(100000);
        statusManager.persistSync();

        statusManager.close();

        check();
    }

    @Test
    public void testSyncRetry() {
        initData();
        mockPersistResult = new CompletableFuture<>();
        mockPersistResult.completeExceptionally(new Exception());

        statusManager.persistSync();
        statusManager.close();
        check();
    }

    @Test
    public void testFail1() {
        initData();
        mockPersistResult = new CompletableFuture<>();
        mockPersistResult.completeExceptionally(new Exception(new StoppedException()));

        assertThrows(RaftException.class, () -> statusManager.persistSync());
    }

    @Test
    public void testFail2() {
        initData();
        mockPersistResult = new CompletableFuture<>();
        mockPersistResult.completeExceptionally(new RuntimeException(new InterruptedException()));

        assertThrows(RaftException.class, () -> statusManager.persistSync());
    }

    @Test
    public void testFail3() {
        initData();
        mockPersistResult = new CompletableFuture<>();
        mockPersistResult.completeExceptionally(new Exception());
        Thread.currentThread().interrupt();

        assertThrows(RaftException.class, () -> statusManager.persistSync());
    }

}
