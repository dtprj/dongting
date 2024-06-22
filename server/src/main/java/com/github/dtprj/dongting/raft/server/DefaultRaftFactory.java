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
import com.github.dtprj.dongting.common.AbstractLifeCircle;
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

/**
 * @author huangli
 */
public abstract class DefaultRaftFactory extends AbstractLifeCircle implements RaftFactory {

    private final RaftServerConfig serverConfig;
    private ExecutorService ioExecutor;
    protected PoolFactory poolFactory;

    public DefaultRaftFactory(RaftServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        this.poolFactory = createPoolFactory();
    }

    protected PoolFactory createPoolFactory() {
        return new DefaultPoolFactory();
    }

    @Override
    protected void doStart() {
        AtomicInteger count = new AtomicInteger();
        ioExecutor = Executors.newFixedThreadPool(serverConfig.getBlockIoThreads(),
                r -> new Thread(r, "raft-io-" + count.incrementAndGet()));
    }

    @Override
    public ExecutorService createBlockIoExecutor() {
        return ioExecutor;
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
        if (ioExecutor != null) {
            ioExecutor.shutdown();
        }
    }

    @Override
    public RaftLog createRaftLog(RaftGroupConfigEx groupConfig, StatusManager statusManager, RaftCodecFactory codecFactory) {
        return new DefaultRaftLog(groupConfig, statusManager, codecFactory);
    }

    @Override
    public SnapshotManager createSnapshotManager(RaftGroupConfigEx groupConfig, StateMachine stateMachine) {
        return new DefaultSnapshotManager(groupConfig, stateMachine);
    }

    @Override
    public Dispatcher createDispatcher(RaftGroupConfig groupConfig) {
        return new Dispatcher("raft-dispatcher-" + groupConfig.getGroupId(), poolFactory,
                groupConfig.getPerfCallback());
    }

    @Override
    public void startDispatcher(Dispatcher dispatcher) {
        dispatcher.start();
    }

    @Override
    public void stopDispatcher(Dispatcher dispatcher, DtTime timeout) {
        dispatcher.stop(timeout);
    }
}
