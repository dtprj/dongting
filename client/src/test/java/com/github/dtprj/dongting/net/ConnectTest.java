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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.common.TestUtil;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class ConnectTest {

    @Test
    public void connectTimeoutAndManualReconnect() throws Exception {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.port = 8888;
        NioServer server = new NioServer(serverConfig);
        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.cleanInterval = 0;
        clientConfig.selectTimeout = 1;
        NioClient client = new NioClient(clientConfig);
        try {
            server.start();
            client.start();

            Peer peer = client.addPeer(new HostPort("127.0.0.1", 8888)).get(1, TimeUnit.SECONDS);
            clientConfig.connectTimeoutMillis = -1;
            try {
                client.connect(peer).get(1, TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                assertInstanceOf(NetTimeoutException.class, e.getCause());
            }

            clientConfig.connectTimeoutMillis = 1000;
            // manual re-connect will success
            client.connect(peer).get(1, TimeUnit.SECONDS);
        } finally {
            TestUtil.stop(client, server);
        }
    }

    @Test
    public void connectAutoReconnect1() throws Exception {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.port = 8888;
        NioServer server = new NioServer(serverConfig);
        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.cleanInterval = 0;
        clientConfig.selectTimeout = 1;
        clientConfig.connectRetryIntervals = new int[]{0};
        NioClient client = new NioClient(clientConfig);
        try {
            server.start();
            client.start();

            Peer peer = client.addPeer(new HostPort("127.0.0.1", 8888)).get(1, TimeUnit.SECONDS);

            clientConfig.connectTimeoutMillis = -1;
            try {
                client.connect(peer).get(1, TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                assertInstanceOf(NetTimeoutException.class, e.getCause());
            }

            clientConfig.connectTimeoutMillis = 1000;
            WaitUtil.waitUtil(() -> peer.getStatus() == PeerStatus.connected);
        } finally {
            TestUtil.stop(client, server);
        }
    }

    @Test
    public void connectAutoReconnect2() throws Exception {
        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.cleanInterval = 0;
        clientConfig.selectTimeout = 1;
        clientConfig.connectRetryIntervals = new int[]{0};
        NioClient client = new NioClient(clientConfig);
        NioServer server = null;
        try {
            client.start();

            Peer peer = client.addPeer(new HostPort("127.0.0.1", 8888)).get(1, TimeUnit.SECONDS);

            clientConfig.connectTimeoutMillis = 1;
            try {
                client.connect(peer).get(1, TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                assertInstanceOf(NetException.class, e.getCause());
            }
            assertTrue(peer.getStatus().ordinal() <= PeerStatus.connecting.ordinal());

            NioServerConfig serverConfig = new NioServerConfig();
            serverConfig.port = 8888;
            server = new NioServer(serverConfig);
            server.start();

            clientConfig.connectTimeoutMillis = 1000;
            WaitUtil.waitUtil(() -> peer.getStatus() == PeerStatus.connected);
        } finally {
            TestUtil.stop(client, server);
        }

    }
}
