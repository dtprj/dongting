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
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
public class StatusManagerTest extends BaseFiberTest {

    private StatusManager statusManager;
    private RaftGroupConfigEx groupConfig;
    private RaftStatusImpl raftStatus;

    public void setup() {
        File dir = TestDir.createTestDir(StatusManagerTest.class.getSimpleName());
        raftStatus = new RaftStatusImpl(dispatcher.getTs());
        groupConfig = new RaftGroupConfigEx(1, "1", "1");
        groupConfig.setDataDir(dir.getAbsolutePath());
        groupConfig.setStatusFile("status.test");
        groupConfig.setRaftStatus(raftStatus);
        groupConfig.setIoExecutor(MockExecutors.ioExecutor());
        groupConfig.setFiberGroup(fiberGroup);
        raftStatus.setTailCache(new TailCache(groupConfig, raftStatus));
        statusManager = new StatusManager(groupConfig);
    }

    private void check() throws Exception {
        Properties p = new Properties();
        File f = new File(groupConfig.getDataDir(), groupConfig.getStatusFile());
        p.load(new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8));

        assertEquals(raftStatus.getCommitIndex() + "", p.getProperty(StatusManager.COMMIT_INDEX_KEY));
        assertEquals(raftStatus.getVotedFor() + "", p.getProperty(StatusManager.VOTED_FOR_KEY));
        assertEquals(raftStatus.getCurrentTerm() + "", p.getProperty(StatusManager.CURRENT_TERM_KEY));
        statusManager.getProperties().forEach((k, v) -> assertEquals(v, p.get(k)));
    }

    @Test
    public void testPersistSyncAndInit() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                setup();
                return Fiber.call(statusManager.initStatusFile(), this::afterInit);
            }
            private FrameCallResult afterInit(Void unused) {
                initData();
                statusManager.persistAsync(true);
                return statusManager.waitForce(this::justReturn);
            }
            @Override
            protected FrameCallResult doFinally() {
                return statusManager.close().await(this::justReturn);
            }
        });
        check();
    }

    private void initData() {
        raftStatus.setCommitIndex(100);
        raftStatus.setVotedFor(200);
        raftStatus.setCurrentTerm(300);
        statusManager.getProperties().setProperty("k1", "v1");
    }

    @Test
    public void testPersistAsyncAndEnsureUpdateFiberFinish() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                setup();
                return Fiber.call(statusManager.initStatusFile(), this::afterInit);
            }
            private FrameCallResult afterInit(Void unused) {
                for (int i = 0; i < 5; i++) {
                    raftStatus.setCommitIndex(100 + i);
                    raftStatus.setVotedFor(200 + i);
                    raftStatus.setCurrentTerm(300 + i);
                    statusManager.getProperties().setProperty("k1", "v1" + i);
                    statusManager.persistAsync(false);
                }
                return statusManager.waitForce(this::justReturn);
            }
            @Override
            protected FrameCallResult doFinally() {
                return statusManager.close().await(this::justReturn);
            }
        });
        check();
    }

    @Test
    public void testMixPersist() throws Exception {

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                setup();
                return Fiber.call(statusManager.initStatusFile(), this::afterInit);
            }
            private FrameCallResult afterInit(Void unused) {
                initData();
                statusManager.persistAsync(false);

                raftStatus.setCommitIndex(100000);
                statusManager.persistAsync(true);
                return statusManager.waitForce(this::justReturn);
            }
            @Override
            protected FrameCallResult doFinally() {
                return statusManager.close().await(this::justReturn);
            }
        });
        check();
    }

}
