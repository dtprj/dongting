package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.raft.sm.SnapshotManager;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.util.concurrent.ExecutorService;

public interface RaftFactory {
    ExecutorService createIoExecutor();

    StateMachine createStateMachine(RaftGroupConfigEx groupConfig);

    RaftLog createRaftLog(RaftGroupConfigEx groupConfig, StatusManager statusManager);

    SnapshotManager createSnapshotManager(RaftGroupConfigEx groupConfig);
}
