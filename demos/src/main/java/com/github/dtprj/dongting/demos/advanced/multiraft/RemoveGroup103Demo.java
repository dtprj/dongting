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
package com.github.dtprj.dongting.demos.advanced.multiraft;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class RemoveGroup103Demo implements GroupId {
    public static void main(String[] args) throws Exception {
        // use replicate port
        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003";  // serverId,ip:replicatePort
        AdminRaftClient adminClient = new AdminRaftClient();
        adminClient.start();
        adminClient.clientAddNode(servers);

        DtTime timeout = new DtTime(5, TimeUnit.SECONDS);
        CompletableFuture<Void> f1 = adminClient.serverRemoveGroup(1, GROUP_ID_103, timeout);
        CompletableFuture<Void> f2 = adminClient.serverRemoveGroup(2, GROUP_ID_103, timeout);
        CompletableFuture<Void> f3 = adminClient.serverRemoveGroup(3, GROUP_ID_103, timeout);
        f1.get();
        f2.get();
        f3.get();

        System.out.println("group 103 added");
        System.exit(0);
    }
}
