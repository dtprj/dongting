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
package com.github.dtprj.dongting.raft;

import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.NioServerConfig;
import com.github.dtprj.dongting.net.Peer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class GroupConManagerTest {

    private static class RG {
        NioServer server;
        NioClient client;
        GroupConManager conManager;
    }

    @Test
    public void testFetch3() throws Exception {
        String servers = "127.0.0.1:6991, 127.0.0.1:6992; 127.0.0.1:6993";
        RG rg1 = null;
        RG rg2 = null;
        RG rg3 = null;
        try {
            rg1 = createNioServer(1, servers, 6991);
            rg2 = createNioServer(2, servers, 6992);
            rg3 = createNioServer(3, servers, 6993);
            fetch3(rg1, rg2, rg3);
            fetch3(rg1, rg2, rg3);
        } finally {
            close(rg1);
            close(rg2);
            close(rg3);
        }
    }

    private void fetch3(RG rg1, RG rg2, RG rg3) throws Exception {
        CompletableFuture<List<Peer>> f1 = rg1.conManager.fetch();
        CompletableFuture<List<Peer>> f2 = rg2.conManager.fetch();
        CompletableFuture<List<Peer>> f3 = rg3.conManager.fetch();
        equals("127.0.0.1:6992,127.0.0.1:6993", f1);
        equals("127.0.0.1:6991,127.0.0.1:6993", f2);
        equals("127.0.0.1:6991,127.0.0.1:6992", f3);
    }

    @Test
    public void testFetch1() throws Exception {
        String servers = "127.0.0.1:6991";
        RG rg1 = null;
        try {
            rg1 = createNioServer(1, servers, 6991);

            CompletableFuture<List<Peer>> f1 = rg1.conManager.fetch();
            assertTrue(f1.get(5, TimeUnit.SECONDS).isEmpty());

            f1 = rg1.conManager.fetch();
            assertTrue(f1.get(5, TimeUnit.SECONDS).isEmpty());
        } finally {
            close(rg1);
        }
    }

    @Test
    public void testFetch2() throws Exception {
        String servers = "127.0.0.1:6991, 127.0.0.1:6992; 127.0.0.1:6993";
        RG rg1 = null;
        RG rg2 = null;
        RG rg3 = null;
        try {
            rg1 = createNioServer(1, servers, 6991);
            rg2 = createNioServer(2, servers, 6992);
            fetch2(rg1, rg2);
            rg3 = createNioServer(3, servers, 6993, false);
            fetch2(rg1, rg2);
        } finally {
            close(rg1);
            close(rg2);
            close(rg3);
        }
    }

    private void fetch2(RG rg1, RG rg2) throws Exception {
        CompletableFuture<List<Peer>> f1 = rg1.conManager.fetch();
        CompletableFuture<List<Peer>> f2 = rg2.conManager.fetch();
        equals("127.0.0.1:6992", f1);
        equals("127.0.0.1:6991", f2);
    }


    private void equals(String servers, CompletableFuture<List<Peer>> f) throws Exception {
        Set<HostPort> s1 = RaftServer.parseServers(servers);
        Set<HostPort> s2 = f.get(5, TimeUnit.SECONDS).stream().map(Peer::getEndPoint).collect(Collectors.toSet());
        assertEquals(s1, s2);
    }

    private void close(RG member) {
        if (member != null) {
            CloseUtil.close(member.client, member.server);
        }
    }

    private RG createNioServer(int id, String servers, int port) {
        return createNioServer(id, servers, port, true);
    }

    private RG createNioServer(int id, String servers, int port, boolean register) {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.setPort(port);
        NioServer server = new NioServer(serverConfig);

        NioClientConfig clientConfig = new NioClientConfig();
        NioClient client = new NioClient(clientConfig);
        GroupConManager manager = new GroupConManager(id, RaftServer.parseServers(servers), servers, client);

        if(register) {
            server.register(Commands.RAFT_HANDSHAKE, manager.getProcessor());
        }

        server.start();
        client.start();
        client.waitStart();

        RG m = new RG();
        m.conManager = manager;
        m.server = server;
        m.client = client;
        return m;
    }

}
