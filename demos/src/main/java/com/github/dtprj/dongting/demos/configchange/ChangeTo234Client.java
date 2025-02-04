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
package com.github.dtprj.dongting.demos.configchange;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class ChangeTo234Client {

    public static void main(String[] args) throws Exception {
        // use replicate port
        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003";
        int groupId = 1;
        AdminRaftClient adminClient = new AdminRaftClient();
        adminClient.start();
        adminClient.addOrUpdateGroup(groupId, servers);
        RaftNode leader = adminClient.fetchLeader(groupId).get();

        DtTime timeout = new DtTime(10, TimeUnit.SECONDS);
        QueryStatusResp status = adminClient.queryRaftServerStatus(
                leader.getNodeId(), groupId, timeout).get();
        if (!status.members.contains(1)) {
            System.out.println("current member is [2,3,4], try run ChangeTo123Client.");
            System.exit(1);
        }

        if (leader.getNodeId() == 1) {
            CompletableFuture<Void> f = adminClient.transferLeader(groupId, leader.getNodeId(), 2, timeout);
            f.get();
            System.out.println("transfer leader to node 2 success");
        }


        CompletableFuture<Long> prepareFuture = adminClient.prepareConfigChange(groupId, Set.of(1, 2, 3), Set.of(),
                Set.of(2, 3, 4), Set.of(), timeout);
        long prepareIndex = prepareFuture.get();
        System.out.println("prepare config change success");

        adminClient.commitChange(groupId, prepareIndex, timeout).get();
        System.out.println("commit change success");

    }
}
