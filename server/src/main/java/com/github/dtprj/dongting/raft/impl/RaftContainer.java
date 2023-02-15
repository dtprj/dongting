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

import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.server.StateMachine;

/**
 * @author huangli
 */
public class RaftContainer {
    private RaftServerConfig config;
    private RaftExecutor raftExecutor;
    private RaftLog raftLog;
    private RaftStatus raftStatus;
    private NioClient client;
    private StateMachine stateMachine;

    public RaftServerConfig getConfig() {
        return config;
    }

    public void setConfig(RaftServerConfig config) {
        this.config = config;
    }

    public RaftExecutor getRaftExecutor() {
        return raftExecutor;
    }

    public void setRaftExecutor(RaftExecutor raftExecutor) {
        this.raftExecutor = raftExecutor;
    }

    public RaftLog getRaftLog() {
        return raftLog;
    }

    public void setRaftLog(RaftLog raftLog) {
        this.raftLog = raftLog;
    }

    public RaftStatus getRaftStatus() {
        return raftStatus;
    }

    public void setRaftStatus(RaftStatus raftStatus) {
        this.raftStatus = raftStatus;
    }

    public NioClient getClient() {
        return client;
    }

    public void setClient(NioClient client) {
        this.client = client;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }
}
