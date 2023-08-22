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
package com.github.dtprj.dongting.raft.test;

import com.github.dtprj.dongting.raft.impl.RaftExecutor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author huangli
 */
public class MockExecutors {
    private static final Thread MOCK_RAFT_THREAD;
    private static final RaftExecutor RAFT_EXECUTOR;
    private static final ExecutorService MOCK_IO_EXECUTOR;

    private static volatile boolean stop = false;

    static {
        MOCK_RAFT_THREAD = new Thread(MockExecutors::runMockRaftThread, "MockRaftThread");
        MOCK_RAFT_THREAD.start();
        RAFT_EXECUTOR = new RaftExecutor(MOCK_RAFT_THREAD);
        MOCK_IO_EXECUTOR = Executors.newSingleThreadExecutor(r -> new Thread(r, "MockIOExecutor"));
    }

    private static void runMockRaftThread() {
        LinkedBlockingQueue<Object> queue = RAFT_EXECUTOR.getQueue();
        while (!stop) {
            try {
                Object obj = queue.take();
                if (obj instanceof Runnable) {
                    ((Runnable) obj).run();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static RaftExecutor raftExecutor() {
        return RAFT_EXECUTOR;
    }

    public static ExecutorService ioExecutor() {
        return MOCK_IO_EXECUTOR;
    }

    @SuppressWarnings("unused")
    public static void stop() {
        MockExecutors.stop = true;
        MOCK_IO_EXECUTOR.shutdown();
    }
}
