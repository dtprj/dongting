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
package com.github.dtprj.dongting.demos.embedded;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.demos.base.DemoClientBase;
import com.github.dtprj.dongting.demos.base.DemoKvServerBase;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.raft.server.RaftServer;

import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class EmbeddedDemo implements GroupId {

    final static int LOOP_COUNT = 1_000_000;

    public static void main(String[] args) throws Exception {
        String replicateServer = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003"; // serverId,ip:replicatePort
        String members = "1,2,3";
        String observers = "";
        RaftServer s1 = DemoKvServerBase.startServer(1, replicateServer, members, observers, new int[]{GROUP_ID});
        RaftServer s2 = DemoKvServerBase.startServer(2, replicateServer, members, observers, new int[]{GROUP_ID});
        RaftServer s3 = DemoKvServerBase.startServer(3, replicateServer, members, observers, new int[]{GROUP_ID});

        // wait raft election finish and servers ready
        s1.getAllGroupReadyFuture().get(60, TimeUnit.SECONDS);
        s2.getAllGroupReadyFuture().get(60, TimeUnit.SECONDS);
        s3.getAllGroupReadyFuture().get(60, TimeUnit.SECONDS);

        System.out.println("-------------------------------------------");
        System.out.println("All servers are ready");
        System.out.println("-------------------------------------------");

        String rpcServers = "1,127.0.0.1:5001;2,127.0.0.1:5002;3,127.0.0.1:5003"; // serverId,ip:servicePort
        KvClient client = DemoClientBase.putAndGetFixCount(GROUP_ID, rpcServers, LOOP_COUNT);


        // System.exit(0);
        client.stop(new DtTime(3, TimeUnit.SECONDS));
        s1.stop(new DtTime(3, TimeUnit.SECONDS));
        s2.stop(new DtTime(3, TimeUnit.SECONDS));
        s3.stop(new DtTime(3, TimeUnit.SECONDS));
    }
}
