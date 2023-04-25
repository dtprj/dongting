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

import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.StateMachine;

/**
 * @author huangli
 */
public class GroupComponents {

    private RaftServerConfig serverConfig;
    private RaftGroupConfig groupConfig;
    private RaftGroupThread raftGroupThread;
    private RaftStatus raftStatus;
    private MemberManager memberManager;
    private VoteManager voteManager;
    private RaftLog raftLog;
    private StateMachine stateMachine;
    private Raft raft;
    private RaftExecutor raftExecutor;
    private ApplyManager applyManager;
    private EventBus eventBus;

    public GroupComponents() {
    }

    public RaftGroupThread getRaftGroupThread() {
        return raftGroupThread;
    }

    public RaftStatus getRaftStatus() {
        return raftStatus;
    }

    public MemberManager getMemberManager() {
        return memberManager;
    }

    public VoteManager getVoteManager() {
        return voteManager;
    }

    public RaftServerConfig getServerConfig() {
        return serverConfig;
    }

    public RaftGroupConfig getGroupConfig() {
        return groupConfig;
    }

    public RaftLog getRaftLog() {
        return raftLog;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public void setServerConfig(RaftServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public void setGroupConfig(RaftGroupConfig groupConfig) {
        this.groupConfig = groupConfig;
    }

    public void setRaftGroupThread(RaftGroupThread raftGroupThread) {
        this.raftGroupThread = raftGroupThread;
    }

    public void setRaftStatus(RaftStatus raftStatus) {
        this.raftStatus = raftStatus;
    }

    public void setMemberManager(MemberManager memberManager) {
        this.memberManager = memberManager;
    }

    public void setVoteManager(VoteManager voteManager) {
        this.voteManager = voteManager;
    }

    public void setRaftLog(RaftLog raftLog) {
        this.raftLog = raftLog;
    }

    public void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    public RaftExecutor getRaftExecutor() {
        return raftExecutor;
    }

    public void setRaftExecutor(RaftExecutor raftExecutor) {
        this.raftExecutor = raftExecutor;
    }

    public Raft getRaft() {
        return raft;
    }

    public void setRaft(Raft raft) {
        this.raft = raft;
    }

    public ApplyManager getApplyManager() {
        return applyManager;
    }

    public void setApplyManager(ApplyManager applyManager) {
        this.applyManager = applyManager;
    }

    public EventBus getEventBus() {
        return eventBus;
    }

    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }
}
