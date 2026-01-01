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
package com.github.dtprj.dongting.dist;

import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyReqPacket;
import com.github.dtprj.dongting.raft.GroupInfo;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;

import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class DistClient extends AdminRaftClient {

    /**
     * Get the leader info for specified group.
     * @param groupId the group id
     * @return future of GroupInfo which contains leader info
     */
    public CompletableFuture<GroupInfo> getLeaderInfo(int groupId) {
        return updateLeaderInfo(groupId, true);
    }

    /**
     * Synchronize group members change, group add/remove, node add/remove to servers.properties file,
     * so make the change effective after node restart.
     * The server side processor only register in Bootstrap class, RaftServer class not register it.
     * @param nodeId the node to invoke
     */
    public CompletableFuture<Void> serverSyncConfig(int nodeId) {
        EmptyBodyReqPacket p = new EmptyBodyReqPacket(Commands.RAFT_ADMIN_SYNC_CONFIG);
        return sendByNodeId(nodeId, createDefaultTimeout(), p);
    }
}
