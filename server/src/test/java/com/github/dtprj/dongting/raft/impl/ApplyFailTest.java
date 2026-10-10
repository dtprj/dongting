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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.ServerTestBase;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.test.Tick;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ApplyFailTest extends ServerTestBase {

    private final ConcurrentLinkedQueue<FiberFuture<Object>> execFutures = new ConcurrentLinkedQueue<>();
    private final CopyOnWriteArrayList<Long> execIndexes = new CopyOnWriteArrayList<>();
    private final AtomicInteger execCount = new AtomicInteger();

    // fail the n-th exec synchronously; 0 means never throw
    private volatile int syncFailExecCount = 0;

    // fail all exec, used to simulate state machine failure during init replay
    private volatile boolean failAllExec = false;

    public ApplyFailTest() {
        super(false);
    }

    @Override
    protected StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
        return new RecordingStateMachine();
    }

    @Test
    void testAsyncExecFailStopApply() throws Exception {
        String servers = "1,127.0.0.1:14401";
        ServerInfo si = createServer(1, servers, "1", "");
        try {
            waitStart(si);
            waitLeaderElectAndGetLeaderId(groupId, si);

            int count = 5;
            for (int i = 0; i < count; i++) {
                si.gc.linearTaskRunner.submitRaftTaskInBizThread(createTask());
            }
            WaitUtil.waitUtil(() -> execIndexes.size() == count);

            List<FiberFuture<Object>> futures = new ArrayList<>(execFutures);
            long idx1 = execIndexes.get(0);

            futures.get(0).fireComplete(null);
            WaitUtil.waitUtil(() -> si.gc.raftStatus.getLastApplied() >= idx1);

            // the state machine exec future fails: the fatal thrown in afterExec is swallowed by
            // FiberFuture.runSimpleCallback(), but Fiber.fatal() has already marked the group fatal,
            // so the shouldStopApply()/drainPendingTasks() checks must stop apply advancing
            futures.get(1).fireCompleteExceptionally(new RaftException("test exec fail"));
            WaitUtil.waitUtil(() -> si.gc.raftStatus.getShareStatus(false).fatalError);
            assertEquals(idx1, si.gc.raftStatus.getLastApplied());

            // complete the rest successfully: lastApplied must not advance over the failed index
            for (int i = 2; i < count; i++) {
                futures.get(i).fireComplete(null);
            }
            Thread.sleep(Tick.tick(5));
            assertEquals(idx1, si.gc.raftStatus.getLastApplied());

            assertNoSnapshotSaved(si);
        } finally {
            waitStop(si);
        }
    }

    @Test
    void testSyncExecFailStopApply() throws Exception {
        String servers = "1,127.0.0.1:14401";
        syncFailExecCount = 2;
        ServerInfo si = createServer(1, servers, "1", "");
        try {
            waitStart(si);
            waitLeaderElectAndGetLeaderId(groupId, si);

            int count = 3;
            for (int i = 0; i < count; i++) {
                si.gc.linearTaskRunner.submitRaftTaskInBizThread(createTask());
            }
            WaitUtil.waitUtil(() -> si.gc.raftStatus.getShareStatus(false).fatalError);

            long idx1 = execIndexes.get(0);
            assertEquals(idx1, si.gc.raftStatus.getLastApplied());

            // the apply fiber died on the fatal, and the apply fiber monitor must not restart it
            Thread.sleep(Tick.tick(5));
            assertEquals(2, execIndexes.size());
            assertEquals(idx1, si.gc.raftStatus.getLastApplied());

            assertNoSnapshotSaved(si);
        } finally {
            waitStop(si);
        }
    }

    @Test
    void testInitReplayFail() throws Exception {
        String servers = "1,127.0.0.1:14401";

        // phase 1: write and apply some logs, then stop without snapshot
        ServerInfo si = createServer(1, servers, "1", "");
        try {
            waitStart(si);
            waitLeaderElectAndGetLeaderId(groupId, si);
            int count = 3;
            for (int i = 0; i < count; i++) {
                si.gc.linearTaskRunner.submitRaftTaskInBizThread(createTask());
            }
            WaitUtil.waitUtil(() -> execIndexes.size() == count);
            List<FiberFuture<Object>> futures = new ArrayList<>(execFutures);
            for (int i = 0; i < count; i++) {
                futures.get(i).fireComplete(null);
            }
            long lastIdx = execIndexes.get(count - 1);
            WaitUtil.waitUtil(() -> si.gc.raftStatus.getLastApplied() >= lastIdx);
        } finally {
            waitStop(si);
        }

        // phase 2: restart; committed logs are replayed and the state machine fails on replay,
        // initFuture must complete exceptionally instead of hanging (RaftServer.start waits it)
        failAllExec = true;
        ServerInfo si2 = createServer(1, servers, "1", "");
        try {
            try {
                si2.gc.raftStatus.initFuture.get(5, TimeUnit.SECONDS);
                throw new AssertionError("initFuture should complete exceptionally");
            } catch (ExecutionException expected) {
            } catch (TimeoutException e) {
                throw new AssertionError("initFuture hangs after apply failure during init replay", e);
            }
        } finally {
            waitStop(si2);
        }
    }

    private void assertNoSnapshotSaved(ServerInfo si) {
        // the failed index must not be solidified by a snapshot
        File snapshotDir = new File(si.gc.groupConfig.dataDir, "snapshot");
        File[] files = snapshotDir.listFiles();
        assertTrue(files == null || files.length == 0, "no snapshot file should be saved");
    }

    private RaftTask createTask() {
        return new RaftTask(RaftReqData.build(LogHeader.TYPE_NORMAL, 0), null, null, null, false,
                new RaftCallback() {
                    @Override
                    public void success(long raftIndex, Object result) {
                    }

                    @Override
                    public void fail(Throwable ex) {
                    }
                });
    }

    private class RecordingStateMachine implements StateMachine {

        @Override
        public FiberFuture<Void> start() {
            return FiberFuture.completedFuture(FiberGroup.currentGroup(), null);
        }

        @Override
        public FiberFuture<Void> stop() {
            return FiberFuture.completedFuture(FiberGroup.currentGroup(), null);
        }

        @Override
        public FiberFuture<Object> exec(RaftInput input) {
            execIndexes.add(input.reqData.index);
            if (failAllExec) {
                throw new RuntimeException("test replay exec fail");
            }
            if (syncFailExecCount > 0) {
                if (execCount.incrementAndGet() == syncFailExecCount) {
                    throw new RuntimeException("test sync exec fail");
                }
                return FiberFuture.completedFuture(FiberGroup.currentGroup(), null);
            }
            FiberFuture<Object> f = FiberGroup.currentGroup().newFuture("manual-exec");
            execFutures.add(f);
            return f;
        }

        @Override
        public FiberFuture<Void> startInstall(boolean clean) {
            throw new RaftException("not expected in this test");
        }

        @Override
        public FiberFuture<Void> installSnapshot(long lastIncludeIndex, int lastIncludeTerm, long offset,
                boolean done, ByteBuffer data) {
            throw new RaftException("not expected in this test");
        }

        @Override
        public FiberFuture<Snapshot> takeSnapshot(SnapshotInfo snapshotInfo) {
            throw new RaftException("not expected in this test");
        }

        @Override
        public void setRaftGroup(RaftGroup raftGroup) {
        }

        @Override
        public DecoderCallback<?> createHeaderCallback(int bizType, DecodeContext context) {
            return null;
        }

        @Override
        public DecoderCallback<?> createBodyCallback(int bizType, DecodeContext context) {
            return null;
        }
    }
}
