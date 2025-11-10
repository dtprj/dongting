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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvClientConfig;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.raft.RaftClientConfig;
import com.github.dtprj.dongting.raft.server.ServerTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.concurrent.TimeUnit;

import static com.github.dtprj.dongting.test.Tick.tick;

/**
 * @author huangli
 */
public class ServerClientLockTest {
    protected static int groupId;
    protected static Server server;
    protected static KvClient client1;
    protected static KvClient client2;
    protected static KvClient client3;

    protected static class Server extends ServerTestBase {
        ServerInfo s1;

        public void startServers() throws Exception {
            String servers = "1,127.0.0.1:4001";
            String members = "1";
            String observers = "";

            s1 = createServer(1, servers, members, observers);
            waitStart(s1);
            waitLeaderElectAndGetLeaderId(groupId, s1);
            ServerClientLockTest.groupId = groupId;
        }

        public void stopServers() {
            waitStop(s1);
        }
    }

    @BeforeAll
    public static void beforeEach() throws Exception {
        server = new Server();
        server.startServers();
        client1 = createClient();
        client2 = createClient();
        client3 = createClient();
    }

    private static KvClient createClient() throws Exception {
        KvClientConfig c = new KvClientConfig();
        c.autoRenewalRetryMillis = new long[]{tick(25), tick(50)};
        KvClient client = new KvClient(c, new RaftClientConfig(), new NioClientConfig());
        client.start();
        client.getRaftClient().clientAddNode("1,127.0.0.1:5001");
        client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1});
        return client;
    }

    @AfterAll
    public static void afterEach() {
        stopClient(client1);
        stopClient(client2);
        stopClient(client3);
        server.stopServers();
    }

    private static void stopClient(KvClient client) {
        if (client != null) {
            client.stop(new DtTime(1, TimeUnit.SECONDS));
        }
    }

}
