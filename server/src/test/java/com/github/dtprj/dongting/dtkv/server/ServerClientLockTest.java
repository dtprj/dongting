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

import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.dtkv.DistributedLockImpl;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvClientConfig;
import com.github.dtprj.dongting.dtkv.LockManager;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftClientConfig;
import com.github.dtprj.dongting.raft.server.ServerTestBase;
import com.github.dtprj.dongting.util.MockRuntimeException;

import java.util.concurrent.TimeUnit;

import static com.github.dtprj.dongting.test.Tick.tick;

/**
 * @author huangli
 */
public class ServerClientLockTest {
    protected static int groupId;
    protected static Server server;
    protected static Client client1;
    protected static Client client2;
    protected static Client client3;

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

    protected static void setupServerClient() throws Exception {
        server = new Server();
        server.startServers();
        client1 = createClient();
        client2 = createClient();
        client3 = createClient();
    }

    protected static class Client extends KvClient {

        protected int sendRpcFailImmediate = 0;
        protected int sendRpcFailWithCallback = 0;

        public Client(KvClientConfig kvClientConfig, RaftClientConfig raftClientConfig, NioClientConfig nioClientConfig) {
            super(kvClientConfig, raftClientConfig, nioClientConfig);
        }

        @Override
        protected LockManager createLockManager() {
            return new LockManager(this) {
                @Override
                protected DistributedLockImpl createLockImpl(int groupId, int lockId, ByteArray key, Runnable expireListener) {
                    return new DistributedLockImpl(lockId, this, groupId, key, expireListener) {
                        @Override
                        protected void sendRpc(WritePacket packet, RpcCallback<Void> callback) {
                            if (sendRpcFailImmediate > 0) {
                                sendRpcFailImmediate--;
                                throw new MockRuntimeException();
                            }
                            if (sendRpcFailWithCallback > 0) {
                                sendRpcFailWithCallback--;
                                FutureCallback.callFail(callback, new MockRuntimeException());
                                return;
                            }
                            super.sendRpc(packet, callback);
                        }
                    };
                }
            };
        }
    }

    private static Client createClient() {
        KvClientConfig c = new KvClientConfig();
        c.autoRenewalRetryMillis = new long[]{tick(5), tick(1000)};
        Client client = new Client(c, new RaftClientConfig(), new NioClientConfig());
        client.start();
        client.getRaftClient().clientAddNode("1,127.0.0.1:5001");
        client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1});
        return client;
    }

    protected static void stopServerClient() {
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
