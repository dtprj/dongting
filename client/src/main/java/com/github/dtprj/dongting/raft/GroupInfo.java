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
package com.github.dtprj.dongting.raft;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class GroupInfo {
    final int groupId;
    final List<RaftNode> servers;
    final RaftNode leader;
    final CompletableFuture<GroupInfo> leaderFuture;
    long epoch;

    public GroupInfo(int groupId, long epoch, List<RaftNode> servers, RaftNode leader, boolean createFuture) {
        this.groupId = groupId;
        this.epoch = epoch;
        this.servers = servers;
        if (createFuture) {
            this.leaderFuture = new CompletableFuture<>();
        } else {
            this.leaderFuture = null;
        }
        this.leader = leader;
    }

    public RaftNode getLeader() {
        return leader;
    }

    public int getGroupId() {
        return groupId;
    }

    public List<RaftNode> getServers() {
        return servers;
    }
}
