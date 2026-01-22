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

import com.github.dtprj.dongting.common.DtTime;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class GroupInfo {

    public final int groupId;
    public final List<RaftNode> servers;
    public final int serversEpoch; //overflow is allowed
    public final RaftNode leader;

    final CompletableFuture<GroupInfo> leaderFuture;
    final DtTime lastLeaderFailTime;
    final DtTime lastFindFailTime;

    GroupInfo(int groupId, List<RaftNode> servers, int serversEpoch, RaftNode leader, boolean createFuture) {
        this.groupId = groupId;
        this.servers = Objects.requireNonNull(servers, "servers");
        this.serversEpoch = serversEpoch;
        if (createFuture) {
            this.leaderFuture = new CompletableFuture<>();
        } else {
            this.leaderFuture = null;
        }
        this.leader = leader;
        this.lastLeaderFailTime = null;
        this.lastFindFailTime = null;
    }

    private GroupInfo(int groupId, List<RaftNode> servers, int serversEpoch, RaftNode leader,
                      CompletableFuture<GroupInfo> leaderFuture, DtTime lastLeaderFailTime,
                      DtTime lastFindFailTime) {
        this.groupId = groupId;
        this.servers = servers;
        this.serversEpoch = serversEpoch;
        this.leader = leader;
        this.leaderFuture = leaderFuture;
        this.lastLeaderFailTime = lastLeaderFailTime;
        this.lastFindFailTime = lastFindFailTime;
    }

    public static GroupInfo createByLastLeaderFailTime(GroupInfo old, DtTime lastLeaderFailTime) {
        return new GroupInfo(old.groupId, old.servers, old.serversEpoch, old.leader, old.leaderFuture,
                lastLeaderFailTime, null);
    }

    public static GroupInfo createByLastFindFailTime(GroupInfo old, DtTime lastFindFailTime) {
        return new GroupInfo(old.groupId, old.servers, old.serversEpoch + 1, null, null,
                null, lastFindFailTime);
    }

    public boolean contains(RaftNode node) {
        for (int size = servers.size(), i = 0; i < size; i++) {
            if (servers.get(i).nodeId == node.nodeId && servers.get(i).hostPort.equals(node.hostPort)) {
                return true;
            }
        }
        return false;
    }
}
