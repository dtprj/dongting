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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.fiber.Dispatcher;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;
import com.github.dtprj.dongting.raft.sm.SnapshotManager;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.util.concurrent.ExecutorService;

/**
 * @author huangli
 */
public interface RaftFactory {

    boolean useSharedIoExecutor();

    ExecutorService createBlockIoExecutor(RaftServerConfig serverConfig, RaftGroupConfigEx groupConfig);

    void shutdownBlockIoExecutor(RaftServerConfig serverConfig, RaftGroupConfigEx groupConfig, ExecutorService executor);

    StateMachine createStateMachine(RaftGroupConfigEx groupConfig);

    RaftLog createRaftLog(RaftGroupConfigEx groupConfig, StatusManager statusManager, RaftCodecFactory codecFactory);

    SnapshotManager createSnapshotManager(RaftGroupConfigEx groupConfig, StateMachine stateMachine, RaftLog raftLog);

    Dispatcher createDispatcher(RaftServerConfig serverConfig, RaftGroupConfig groupConfig);

    void startDispatcher(Dispatcher dispatcher);

    void stopDispatcher(Dispatcher dispatcher, DtTime timeout);

    RaftGroupConfig createConfig(int groupId, String nodeIdOfMembers, String nodeIdOfObservers);
}
