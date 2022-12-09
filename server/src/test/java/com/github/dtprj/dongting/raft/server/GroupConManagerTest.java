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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.NioServerConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class GroupConManagerTest {

    private static class RN {
        NioServer server;
        NioClient client;
        GroupConManager conManager;
        Set<HostPort> servers;
    }

    @Test
    public void testFetch3() throws Exception {
        String servers = "127.0.0.1:6991, 127.0.0.1:6992; 127.0.0.1:6993";
        RN rn1 = null;
        RN rn2 = null;
        RN rn3 = null;
        try {
            rn1 = createRaftNode(1, servers, 6991);
            rn2 = createRaftNode(2, servers, 6992);
            rn3 = createRaftNode(3, servers, 6993);
            fetch3(rn1, rn2, rn3);
            fetch3(rn1, rn2, rn3);
        } finally {
            close(rn1);
            close(rn2);
            close(rn3);
        }
    }

    private void fetch3(RN r1, RN r2, RN r3) throws Exception {
        CompletableFuture<List<RaftMember>> f1 = r1.conManager.connect(r1.servers).thenCompose(list -> r1.conManager.fetch(list));
        CompletableFuture<List<RaftMember>> f2 = r2.conManager.connect(r2.servers).thenCompose(list -> r2.conManager.fetch(list));
        CompletableFuture<List<RaftMember>> f3 = r3.conManager.connect(r3.servers).thenCompose(list -> r3.conManager.fetch(list));
        equals(r1, f1, 1, 3);
        equals(r2, f2, 2, 3);
        equals(r3, f3, 3, 3);
    }

    @Test
    public void testFetch2() throws Exception {
        String servers = "127.0.0.1:6991, 127.0.0.1:6992; 127.0.0.1:6993";
        RN rg1 = null;
        RN rg2 = null;
        RN rg3 = null;
        try {
            rg1 = createRaftNode(1, servers, 6991);
            rg2 = createRaftNode(2, servers, 6992);
            fetch2(rg1, rg2);
            rg3 = createRaftNode(3, servers, 6993, false);
            fetch2(rg1, rg2);
        } finally {
            close(rg1);
            close(rg2);
            close(rg3);
        }
    }

    private void fetch2(RN r1, RN r2) throws Exception {
        CompletableFuture<List<RaftMember>> f1 = r1.conManager.connect(r1.servers).thenCompose(list -> r1.conManager.fetch(list));
        CompletableFuture<List<RaftMember>> f2 = r2.conManager.connect(r2.servers).thenCompose(list -> r2.conManager.fetch(list));
        equals(r1, f1, 1, 2);
        equals(r2, f2, 2, 2);
    }


    private void equals(RN r1, CompletableFuture<List<RaftMember>> f, int id, int expectSize) throws Exception {
        Set<HostPort> s1 = r1.servers;
        List<RaftMember> s2 = f.get(5, TimeUnit.SECONDS);
        assertEquals(expectSize, s2.size());
        for (RaftMember m : s2) {
            assertTrue(s1.containsAll(m.getServers()));
            assertTrue(m.getId() > 0);
            assertNotNull(m.getPeer());
            if (id == m.getId()) {
                assertTrue(m.isSelf());
            }
        }
    }

    private void close(RN member) {
        if (member != null) {
            CloseUtil.close(member.client, member.server);
        }
    }

    private RN createRaftNode(int id, String servers, int port) {
        return createRaftNode(id, servers, port, true);
    }

    private RN createRaftNode(int id, String servers, int port, boolean register) {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.setPort(port);
        NioServer server = new NioServer(serverConfig);

        NioClientConfig clientConfig = new NioClientConfig();
        NioClient client = new NioClient(clientConfig);
        GroupConManager manager = new GroupConManager(id, servers, client);

        if (register) {
            server.register(Commands.RAFT_HANDSHAKE, manager.getProcessor());
        }

        server.start();
        client.start();
        client.waitStart();

        RN rn = new RN();
        rn.conManager = manager;
        rn.server = server;
        rn.client = client;
        rn.servers = RaftServer.parseServers(servers);
        return rn;
    }

}
