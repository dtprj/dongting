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

import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.ServerTestBase;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
public class LinearTaskRunnerStopTest extends ServerTestBase {

    @Test
    public void testAllTaskCallbacksInvokedWhenStop() throws Exception {
        String servers = "1,127.0.0.1:14401";
        ServerInfo si = createServer(1, servers, "1", "");
        try {
            waitStart(si);
            waitLeaderElectAndGetLeaderId(groupId, si);

            int count = 2000;
            AtomicInteger invoked = new AtomicInteger();
            RaftCallback callback = new RaftCallback() {
                @Override
                public void success(long raftIndex, Object result) {
                    invoked.incrementAndGet();
                }

                @Override
                public void fail(Throwable ex) {
                    invoked.incrementAndGet();
                }
            };
            LinearTaskRunner runner = si.gc.linearTaskRunner;
            for (int i = 0; i < count / 2; i++) {
                runner.submitRaftTaskInBizThread(createTask(callback));
            }
            si.gc.fiberGroup.requestShutdown();
            for (int i = 0; i < count / 2; i++) {
                runner.submitRaftTaskInBizThread(createTask(callback));
            }
            WaitUtil.waitUtil(() -> invoked.get() == count);
            assertEquals(count, invoked.get());
            si.gc.fiberGroup.shutdownFuture.get(5, TimeUnit.SECONDS);
        } finally {
            waitStop(si);
        }
    }

    private RaftTask createTask(RaftCallback callback) {
        return new RaftTask(RaftReqData.build(LogHeader.TYPE_HEARTBEAT, 0), null, null, null, false, callback);
    }
}
