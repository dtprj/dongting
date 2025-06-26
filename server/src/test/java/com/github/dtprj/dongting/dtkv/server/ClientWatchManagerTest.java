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
import com.github.dtprj.dongting.dtkv.ClientWatchManager;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvListener;
import com.github.dtprj.dongting.dtkv.WatchEvent;
import com.github.dtprj.dongting.raft.server.ServerTestBase;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
public class ClientWatchManagerTest implements KvListener {

    private static Server server;
    private static int groupId;
    private KvClient client;
    private ClientWatchManager manager;
    private ConcurrentLinkedQueue<WatchEvent> events;

    @Override
    public void onUpdate(WatchEvent event) {
        events.add(event);
    }

    @BeforeAll
    public static void initServer() throws Exception {
        server = new Server();
        server.startServers();
    }

    @AfterAll
    public static void stopServer() throws Exception {
        server.stopServers();
    }

    private static class Server extends ServerTestBase {
        ServerInfo s1;
        ServerInfo s2;
        ServerInfo s3;

        public void startServers() throws Exception {
            String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003";
            String members = "1,2,3";
            String observers = "";

            s1 = createServer(1, servers, members, observers);
            s2 = createServer(2, servers, members, observers);
            s3 = createServer(3, servers, members, observers);
            waitStart(s1);
            waitStart(s2);
            waitStart(s3);
            waitLeaderElectAndGetLeaderId(groupId, s1, s2, s3);
            ClientWatchManagerTest.groupId = groupId;
        }

        public void stopServers() {
            waitStop(s1);
            waitStop(s2);
            waitStop(s3);
        }

        @Override
        protected void config(KvConfig config) {
            config.watchDispatchIntervalMillis = 1;
        }
    }

    @BeforeEach
    public void setup() {
        events = new ConcurrentLinkedQueue<>();
        client = new KvClient();
        client.start();
        client.getRaftClient().clientAddNode("1,127.0.0.1:5001;2,127.0.0.1:5002;3,127.0.0.1:5003");
        client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1, 2, 3});
        manager = client.getClientWatchManager();
        manager.setListener(this);
    }

    @AfterEach
    public void teardown() {
        client.stop(new DtTime(1, TimeUnit.SECONDS), true);
    }

    @Test
    public void testAddRemoveWatch() {
        long idx1 = client.put(groupId, "key1".getBytes(), "value1".getBytes());
        long idx2 = client.put(groupId, "key2".getBytes(), "value2".getBytes());
        manager.addWatch(groupId, "key1", "key2");
        TestUtil.waitUtil(2, () -> events.size());
        WatchEvent e = events.poll();
        assertEquals(idx1, e.raftIndex);
        assertEquals("key1", e.key.toString());
        assertEquals("value1", new String(e.value));
        assertEquals(idx2, e.raftIndex);
        assertEquals("key2", e.key.toString());
        assertEquals("value2", new String(e.value));

        // key1 is readd
//        manager.addWatch(groupId, "key1");
//        idx1 = client.put(groupId, "key1".getBytes(), "value1_2".getBytes());
//        idx2 = client.put(groupId, "key2".getBytes(), "value2_2".getBytes());
//        TestUtil.waitUtil(1, () -> events.size());
//        e = events.poll();
//        assertEquals(idx1, e.raftIndex);
//        assertEquals("key1", e.key.toString());
//        assertEquals("value1_2", new String(e.value));
//        e = events.poll();
//        assertEquals(idx2, e.raftIndex);
//        assertEquals("key2", e.key.toString());
//        assertEquals("value2_2", new String(e.value));
    }
}
