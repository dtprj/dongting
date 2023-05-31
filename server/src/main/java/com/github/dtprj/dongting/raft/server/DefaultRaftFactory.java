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

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.raft.sm.DefaultSnapshotManager;
import com.github.dtprj.dongting.raft.sm.SnapshotManager;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.DefaultRaftLog;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author huangli
 */
public class DefaultRaftFactory extends AbstractLifeCircle implements RaftFactory {

    private final RaftServerConfig serverConfig;
    private ExecutorService ioExecutor;

    public DefaultRaftFactory(RaftServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    @Override
    protected void doStart() {
        ioExecutor = createIoExecutor();
    }

    protected ExecutorService createIoExecutor() {
        AtomicInteger count = new AtomicInteger();
        return Executors.newFixedThreadPool(serverConfig.getIoThreads(),
                r -> new Thread(r, "raft-io-" + count.incrementAndGet()));
    }

    @Override
    protected void doStop() {
        if (ioExecutor != null) {
            ioExecutor.shutdown();
        }
    }

    @Override
    public StateMachine<?, ?, ?> createStateMachine(RaftGroupConfigEx groupConfig) {
        return null;
    }

    @Override
    public RaftLog createRaftLog(RaftGroupConfigEx groupConfig) {
        return new DefaultRaftLog(groupConfig, ioExecutor);
    }

    @Override
    public SnapshotManager createSnapshotManager(RaftGroupConfigEx groupConfig) {
        return new DefaultSnapshotManager(groupConfig, ioExecutor);
    }
}
