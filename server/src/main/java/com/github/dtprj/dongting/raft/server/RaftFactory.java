package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.fiber.Dispatcher;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;
import com.github.dtprj.dongting.raft.sm.SnapshotManager;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public interface RaftFactory {
    ExecutorService createBlockIoExecutor(RaftServerConfig serverConfig, RaftGroupConfigEx groupConfig);

    void shutdownBlockIoExecutor(RaftServerConfig serverConfig, RaftGroupConfigEx groupConfig, ExecutorService executor);

    StateMachine createStateMachine(RaftGroupConfigEx groupConfig);

    RaftLog createRaftLog(RaftGroupConfigEx groupConfig, StatusManager statusManager, RaftCodecFactory codecFactory);

    SnapshotManager createSnapshotManager(RaftGroupConfigEx groupConfig, StateMachine stateMachine, Consumer<Long> logDeleter);

    Dispatcher createDispatcher(RaftServerConfig serverConfig, RaftGroupConfig groupConfig);

    void startDispatcher(Dispatcher dispatcher);

    void stopDispatcher(Dispatcher dispatcher, DtTime timeout);

    RaftGroupConfig createConfig(int groupId, String nodeIdOfMembers, String nodeIdOfObservers);
}
