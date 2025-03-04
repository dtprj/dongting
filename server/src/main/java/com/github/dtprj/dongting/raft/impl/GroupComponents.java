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

import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.SnapshotManager;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

/**
 * @author huangli
 */
public class GroupComponents {
    public RaftServerConfig serverConfig;
    public RaftGroupConfigEx groupConfig;
    public RaftStatusImpl raftStatus;
    public MemberManager memberManager;
    public VoteManager voteManager;
    public LinearTaskRunner linearTaskRunner;
    public CommitManager commitManager;
    public ApplyManager applyManager;
    public SnapshotManager snapshotManager;
    public StatusManager statusManager;
    public ReplicateManager replicateManager;

    public NodeManager nodeManager;
    public PendingStat serverStat;

    public RaftLog raftLog;
    public StateMachine stateMachine;

    public FiberGroup fiberGroup;

    public final IntObjMap<FiberChannel<Object>> processorChannels = new IntObjMap<>();

}
