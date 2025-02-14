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
public class ChangeTo123Client implements GroupId {

    // to run this demo, you need to start ConfigChangeDemoServer1/2/3/4 first.
    // this demo change raft member from [2, 3, 4] to [1, 2, 3], if current member is [1, 2, 3], exit with code 1.
    public static void main(String[] args) throws Exception {
        // use replicate port
        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003;4,127.0.0.1:4004";
        AdminRaftClient adminClient = new AdminRaftClient();
        adminClient.start();
        adminClient.clientAddNode(servers);
        adminClient.clientAddOrUpdateGroup(GROUP_ID, new int[]{1, 2, 3, 4});
        RaftNode leader = adminClient.fetchLeader(GROUP_ID).get();

        DtTime timeout = new DtTime(10, TimeUnit.SECONDS);
        QueryStatusResp status = adminClient.queryRaftServerStatus(
                leader.getNodeId(), GROUP_ID, timeout).get();
        if (status.members.contains(1)) {
            System.out.println("current member is [1,2,3], try run ChangeTo123Client.");
            System.exit(1);
        }

        if (leader.getNodeId() == 4) {
            CompletableFuture<Void> f = adminClient.transferLeader(GROUP_ID, leader.getNodeId(), 2, timeout);
            f.get();
            System.out.println("transfer leader to node 2 success");
        }

        // Prepared member will also participate in the election and the calculation of replicating the quorum.
        // Therefore, the best practice is to make two changes. First, add it to the raft group as observers,
        // and then promote it to members through another change.
        // For simplicity here, the new node are directly added as member to the raft group.
        CompletableFuture<Long> prepareFuture = adminClient.prepareConfigChange(GROUP_ID, Set.of(2, 3, 4), Set.of(),
                Set.of(1, 2, 3), Set.of(), timeout);
        long prepareIndex = prepareFuture.get();
        System.out.println("prepare config change success");

        adminClient.commitChange(GROUP_ID, prepareIndex, timeout).get();
        System.out.println("commit change success");

        System.exit(0);
    }
}
