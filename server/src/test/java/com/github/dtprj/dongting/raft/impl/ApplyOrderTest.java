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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
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
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class ApplyOrderTest extends ServerTestBase {

    private final ConcurrentLinkedQueue<FiberFuture<Object>> execFutures = new ConcurrentLinkedQueue<>();
    private final CopyOnWriteArrayList<Long> execIndexes = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Long> appliedIndexes = new CopyOnWriteArrayList<>();
    private final AtomicInteger invoked = new AtomicInteger();

    public ApplyOrderTest() {
        super(false);
    }

    @Override
    protected StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
        return new ManualStateMachine();
    }

    @Test
    void testApplyOrderWhenExecCompleteOutOfOrder() throws Exception {
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

            List<Long> sorted = new ArrayList<>(execIndexes);
            sorted.sort(null);
            assertEquals(sorted, execIndexes);
            for (int i = 1; i < count; i++) {
                assertTrue(execIndexes.get(i) > execIndexes.get(i - 1));
            }
            long idx1 = execIndexes.get(0);
            long idx2 = execIndexes.get(1);
            long idx3 = execIndexes.get(2);
            long idx4 = execIndexes.get(3);
            long idx5 = execIndexes.get(4);

            List<FiberFuture<Object>> futures = new ArrayList<>(execFutures);

            futures.get(4).fireComplete(null);
            assertEquals(idx1 - 1, si.gc.raftStatus.getLastApplied());

            futures.get(0).fireComplete(null);
            WaitUtil.waitUtil(() -> si.gc.raftStatus.getLastApplied() >= idx1);
            assertTrue(si.gc.raftStatus.getLastApplied() < idx2);

            futures.get(2).fireComplete(null);
            assertTrue(si.gc.raftStatus.getLastApplied() < idx2);

            futures.get(1).fireComplete(null);
            WaitUtil.waitUtil(() -> si.gc.raftStatus.getLastApplied() >= idx3);
            assertTrue(si.gc.raftStatus.getLastApplied() < idx4);

            futures.get(3).fireComplete(null);
            WaitUtil.waitUtil(() -> si.gc.raftStatus.getLastApplied() >= idx5);
            assertEquals(execIndexes, appliedIndexes);
        } finally {
            waitStop(si);
        }
    }

    @Test
    void testFlowControl() throws Exception {
        String servers = "1,127.0.0.1:14401";
        ServerInfo si = createServer(1, servers, "1", "");
        try {
            waitStart(si);
            waitLeaderElectAndGetLeaderId(groupId, si);

            int count = 2100;
            for (int i = 0; i < count; i++) {
                si.gc.linearTaskRunner.submitRaftTaskInBizThread(createTask());
            }

            WaitUtil.waitUtil(() -> si.gc.applyManager.pendingTasks.size() >= ApplyManager.MAX_PENDING_TASKS);
            int cur = execIndexes.size();
            assertTrue(cur < count, "apply fiber should be blocked by flow control, exec=" + cur);
            assertTrue(cur > ApplyManager.MAX_PENDING_TASKS - 200, "exec=" + cur);
            assertEquals(0, invoked.get());
            assertEquals(0, appliedIndexes.size());

            WaitUtil.waitUtil(() -> {
                FiberFuture<Object> f;
                while ((f = execFutures.poll()) != null) {
                    f.fireComplete(null);
                }
                return invoked.get() == count && execIndexes.size() == count;
            });
            assertEquals(execIndexes, appliedIndexes);
        } finally {
            waitStop(si);
        }
    }

    private RaftTask createTask() {
        return new RaftTask(RaftReqData.build(LogHeader.TYPE_NORMAL, 0), null, null, null, false,
                new RaftCallback() {
                    @Override
                    public void success(long raftIndex, Object result) {
                        invoked.incrementAndGet();
                        appliedIndexes.add(raftIndex);
                    }

                    @Override
                    public void fail(Throwable ex) {
                        invoked.incrementAndGet();
                    }
                });
    }

    private class ManualStateMachine extends AbstractLifeCircle implements StateMachine {

        @Override
        protected void doStart() {
        }

        @Override
        protected void doStop(DtTime timeout, boolean mayNotStart) {
        }

        @Override
        public FiberFuture<Object> exec(RaftInput input) {
            FiberFuture<Object> f = FiberGroup.currentGroup().newFuture("manual-exec");
            execFutures.add(f);
            execIndexes.add(input.reqData.index);
            return f;
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
