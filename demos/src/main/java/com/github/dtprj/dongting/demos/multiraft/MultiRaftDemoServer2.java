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
package com.github.dtprj.dongting.demos.multiraft;

import com.github.dtprj.dongting.demos.base.DemoKvServerBase;

/**
 * @author huangli
 */
public class MultiRaftDemoServer2 extends DemoKvServerBase implements GroupId {
    public static void main(String[] args) {
        int nodeId = 2;
        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003"; // serverId,ip:replicatePort
        String members = "1,2,3";
        String observers = "";
        startServer(nodeId, servers, members, observers, new int[]{GROUP_ID_101, GROUP_ID_102});
    }
}
