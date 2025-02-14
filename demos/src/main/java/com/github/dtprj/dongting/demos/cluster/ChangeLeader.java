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
package com.github.dtprj.dongting.demos.cluster;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class ChangeLeader implements GroupId {

    public static void main(String[] args) throws Exception {
        // use replicate port
        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003";
        AdminRaftClient adminClient = new AdminRaftClient();
        adminClient.start();
        adminClient.clientAddNode(servers);
        adminClient.clientAddOrUpdateGroup(GROUP_ID, new int[]{1, 2, 3});
        RaftNode leader = adminClient.fetchLeader(GROUP_ID).get();

        System.out.println("current leader is node " + leader.getNodeId());
        int newLeaderNodeId = leader.getNodeId() == 1 ? 2 : 1;
        DtTime timeout = new DtTime(5, TimeUnit.SECONDS);
        CompletableFuture<Void> f = adminClient.transferLeader(GROUP_ID, leader.getNodeId(), newLeaderNodeId, timeout);
        f.get();
        System.out.println("transfer leader success");
    }
}
