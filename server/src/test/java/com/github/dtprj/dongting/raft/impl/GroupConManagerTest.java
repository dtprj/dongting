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
package com.github.dtprj.dongting.raft.impl;

/*
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.NioServerConfig;
import com.github.dtprj.dongting.raft.server.RaftException;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
*/
/**
 * @author huangli
 */
public class GroupConManagerTest {

    /*
    private static class RN {
        NioServer server;
        NioClient client;
        GroupConManager conManager;
        Set<HostPort> servers;
    }

    private static ExecutorService executor;

    @BeforeAll
    public static void beforeAll() {
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterAll
    public static void afterAll() {
        executor.shutdown();
    }

    private static RN createRaftNode(int id, String servers, int port) {
        return createRaftNode(id, servers, port, true);
    }

    private static RN createRaftNode(int id, String servers, int port, boolean register) {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.setPort(port);
        serverConfig.setIoThreads(1);
        serverConfig.setName("RaftServer" + id);
        NioServer server = new NioServer(serverConfig);

        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.setName("RaftClient" + id);
        NioClient client = new NioClient(clientConfig);
        RaftServerConfig config = new RaftServerConfig();
        config.setServers(servers);
        config.setId(id);
        GroupConManager manager = new GroupConManager(config, client, executor, new RaftStatus(2, 2));

        if (register) {
            server.register(Commands.RAFT_PING, manager.getProcessor());
        }

        server.start();
        client.start();
        client.waitStart();

        RN rn = new RN();
        rn.conManager = manager;
        rn.server = server;
        rn.client = client;
        rn.servers = RaftUtil.parseServers(servers);
        return rn;
    }

    @Test
    public void testInitRaftGroup3() throws Exception {
        String servers = "127.0.0.1:6991, 127.0.0.1:6992; 127.0.0.1:6993";
        try (
                InitThread t1 = new InitThread(1, servers, 6991, 2);
                InitThread t2 = new InitThread(2, servers, 6992, 2);
                InitThread t3 = new InitThread(3, servers, 6993, 2);
        ) {
            t1.start();
            Thread.sleep(1);
            t2.start();
            Thread.sleep(1);
            t3.start();
            t1.join(10000);
            t2.join(10000);
            t3.join(10000);
            assertEquals(Boolean.TRUE, t1.result);
            assertEquals(Boolean.TRUE, t2.result);
            assertEquals(Boolean.TRUE, t3.result);
        }
    }

    @Test
    public void testInitRaftGroup2() throws Exception {
        String servers = "127.0.0.1:6991, 127.0.0.1:6992; 127.0.0.1:6993";
        try (
                InitThread t1 = new InitThread(1, servers, 6991, 2);
                InitThread t2 = new InitThread(2, servers, 6992, 2);
        ) {
            t1.start();
            Thread.sleep(1);
            t2.start();

            t1.join(10000);
            t2.join(10000);

            assertEquals(Boolean.TRUE, t1.result);
            assertEquals(Boolean.TRUE, t2.result);
        }
    }

    @Test
    public void testInitRaftGroup1() throws Exception {
        String servers = "127.0.0.1:6991";
        try (InitThread t1 = new InitThread(1, servers, 6991, 1)) {
            t1.start();

            t1.join(10000);

            assertEquals(Boolean.TRUE, t1.result);
        }
    }

    @Test
    public void testInitRaftGroupFail3_1() throws Exception {
        String servers = "127.0.0.1:6991, 127.0.0.1:6992; 127.0.0.1:6993";
        try (
                InitThread t1 = new InitThread(1, servers, 6991, 2);
                InitThread t2 = new InitThread(1, servers, 6992, 2);
                InitThread t3 = new InitThread(1, servers, 6993, 2);
        ) {
            t1.start();
            Thread.sleep(1);
            t2.start();
            Thread.sleep(1);
            t3.start();
            t1.join(10000);
            t2.join(10000);
            t3.join(10000);
            assertTrue(t1.result instanceof RaftException);
            assertTrue(t2.result instanceof RaftException);
            assertTrue(t3.result instanceof RaftException);
        }
    }

    @Test
    public void testInitRaftGroupFail3_2() throws Exception {
        String servers = "127.0.0.1:6991, 127.0.0.1:6992; 127.0.0.1:6993";
        try (
                InitThread t1 = new InitThread(1, servers, 6991, 2);
                InitThread t2 = new InitThread(2, "127.0.0.1:6991, 127.0.0.1:6993", 6992, 2);
                InitThread t3 = new InitThread(3, "127.0.0.1:6991; 127.0.0.1:6992", 6993, 2);
        ) {
            t1.start();
            Thread.sleep(1);
            t2.start();
            Thread.sleep(1);
            t3.start();
            t1.join(10000);
            t2.join(10000);
            t3.join(10000);
            assertTrue(t1.result instanceof RaftException);
            assertTrue(t2.result instanceof RaftException);
            assertTrue(t3.result instanceof RaftException);
        }
    }

    @Test
    public void testInitRaftGroupFail3_3() throws Exception {
        String servers = "127.0.0.1:6991, 127.0.0.1:6992";
        try (
                InitThread t1 = new InitThread(1, servers, 6991, 2);
                InitThread t2 = new InitThread(2, servers, 6992, 2);
                InitThread t3 = new InitThread(3, "127.0.0.1:6991, 127.0.0.1:6992; 127.0.0.1:6993", 6993, 2);
        ) {
            t1.start();
            Thread.sleep(1);
            t2.start();
            Thread.sleep(1);
            t3.start();
            t1.join(10000);
            t2.join(10000);
            t3.join(10000);
            assertEquals(Boolean.TRUE, t1.result);
            assertEquals(Boolean.TRUE, t2.result);
            assertTrue(t3.result instanceof RaftException);
        }
    }

    private static class InitThread extends Thread implements AutoCloseable {

        private final int id;
        private final String servers;
        private final int port;
        private final int quorum;
        private Object result;
        private RN rn;

        public InitThread(int id, String servers, int port, int quorum) {
            this.id = id;
            this.servers = servers;
            this.port = port;
            this.quorum = quorum;
        }

        @Override
        public void run() {
            try {
                rn = createRaftNode(id, servers, port);
                rn.conManager.initRaftGroup(quorum, rn.servers, 1);
                result = Boolean.TRUE;
            } catch (Exception e) {
                result = e;
            }
        }

        @Override
        public void close() {
            if (rn != null) {
                CloseUtil.close(rn.client, rn.server);
            }
        }
    }
*/
}
