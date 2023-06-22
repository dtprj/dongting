package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.raft.sm.SnapshotManager;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;

public interface RaftFactory {
    StateMachine createStateMachine(RaftGroupConfigEx groupConfig);

    RaftLog createRaftLog(RaftGroupConfigEx groupConfig);

    SnapshotManager createSnapshotManager(RaftGroupConfigEx groupConfig);
}
