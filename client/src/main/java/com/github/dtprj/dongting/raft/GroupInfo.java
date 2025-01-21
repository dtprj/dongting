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

import com.github.dtprj.dongting.net.Peer;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
class GroupInfo {
    final int groupId;
    final List<RaftNode> servers;
    final Peer leader;
    final RaftNode leaderNodeInfo;
    final CompletableFuture<GroupInfo> leaderFuture;
    long epoch;

    public GroupInfo(int groupId, long epoch, List<RaftNode> servers, Peer leader, boolean createFuture) {
        this.groupId = groupId;
        this.epoch = epoch;
        this.servers = servers;
        this.leader = leader;
        if (createFuture) {
            this.leaderFuture = new CompletableFuture<>();
        } else {
            this.leaderFuture = null;
        }
        RaftNode ni = null;
        if (leader != null) {
            for (RaftNode nodeInfo : servers) {
                if (nodeInfo.getPeer() == leader) {
                    ni = nodeInfo;
                }
            }
            if (ni == null) {
                throw new IllegalArgumentException("leader not in servers");
            } else {
                this.leaderNodeInfo = ni;
            }
        } else {
            this.leaderNodeInfo = null;
        }

    }
}
