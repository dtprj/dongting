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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.buf.PoolFactory;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.fiber.Dispatcher;
import com.github.dtprj.dongting.raft.sm.DefaultSnapshotManager;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;
import com.github.dtprj.dongting.raft.sm.SnapshotManager;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.DefaultRaftLog;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * @author huangli
 */
public abstract class DefaultRaftFactory implements RaftFactory {

    private final ReentrantLock lock = new ReentrantLock();

    private int executorUseCount;
    private ExecutorService sharedIoExecutor;

    protected PoolFactory poolFactory;

    public DefaultRaftFactory() {
        this.poolFactory = createPoolFactory();
    }

    protected PoolFactory createPoolFactory() {
        return new DefaultPoolFactory();
    }

    @Override
    public ExecutorService createBlockIoExecutor(RaftServerConfig serverConfig, RaftGroupConfigEx groupConfig) {
        lock.lock();
        try {
            if (sharedIoExecutor == null) {
                executorUseCount = 1;
                AtomicInteger count = new AtomicInteger();
                sharedIoExecutor = Executors.newFixedThreadPool(serverConfig.blockIoThreads,
                        r -> new Thread(r, "raft-io-" + count.incrementAndGet()));
            } else {
                executorUseCount++;
            }
            return sharedIoExecutor;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdownBlockIoExecutor(RaftServerConfig serverConfig, RaftGroupConfigEx groupConfig, ExecutorService executor) {
        lock.lock();
        try {
            executorUseCount--;
            if (executorUseCount == 0) {
                sharedIoExecutor.shutdown();
                sharedIoExecutor = null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RaftLog createRaftLog(RaftGroupConfigEx groupConfig, StatusManager statusManager, RaftCodecFactory codecFactory) {
        return new DefaultRaftLog(groupConfig, statusManager, codecFactory);
    }

    @Override
    public SnapshotManager createSnapshotManager(RaftGroupConfigEx groupConfig, StateMachine stateMachine, RaftLog raftLog) {
        Consumer<Long> logDeleter = lastIncludeIndex-> raftLog.markTruncateByIndex(
                lastIncludeIndex, groupConfig.autoDeleteLogDelayMillis);
        return new DefaultSnapshotManager(groupConfig, stateMachine, logDeleter);
    }

    @Override
    public Dispatcher createDispatcher(RaftServerConfig serverConfig, RaftGroupConfig groupConfig) {
        return new Dispatcher("raft-dispatcher-" + groupConfig.groupId, poolFactory,
                groupConfig.perfCallback);
    }

    @Override
    public void startDispatcher(Dispatcher dispatcher) {
        dispatcher.start();
    }

    @Override
    public void stopDispatcher(Dispatcher dispatcher, DtTime timeout) {
        dispatcher.stop(timeout);
    }

    @Override
    public RaftGroupConfig createConfig(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
        throw new UnsupportedOperationException();
    }
}
