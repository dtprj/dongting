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
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvListener;
import com.github.dtprj.dongting.dtkv.KvReq;
import com.github.dtprj.dongting.dtkv.KvStatusResp;
import com.github.dtprj.dongting.dtkv.WatchEvent;
import com.github.dtprj.dongting.dtkv.WatchNotify;
import com.github.dtprj.dongting.dtkv.WatchNotifyReq;
import com.github.dtprj.dongting.dtkv.WatchReq;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.ServerTestBase;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.test.Tick;
import com.github.dtprj.dongting.test.WaitUtil;
import com.github.dtprj.dongting.util.MockRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class ClientWatchManagerTest implements KvListener {

    private static Server server;
    private static int groupId;
    private KvClient client;
    private MockClientWatchManager manager;
    private ConcurrentLinkedQueue<WatchEvent> events;

    private AtomicInteger mockSyncExCount;
    private AtomicInteger mockQueryStatusExCount;

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
    public static void stopServer() {
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
            ServerInfo leader = waitLeaderElectAndGetLeaderId(groupId, s1, s2, s3);
            ClientWatchManagerTest.groupId = groupId;
            // add first item and make raft index in statemachine greater than 0
            KvReq req = new KvReq(groupId, "aaa".getBytes(), "bbb".getBytes());
            RaftInput i = new RaftInput(DtKV.BIZ_TYPE_PUT, null, req,
                    new DtTime(3, TimeUnit.SECONDS), false);
            CompletableFuture<Long> f = new CompletableFuture<>();
            leader.raftServer.getRaftGroup(groupId).submitLinearTask(i, new RaftCallback() {
                @Override
                public void success(long raftIndex, Object result) {
                    f.complete(raftIndex);
                }

                @Override
                public void fail(Throwable ex) {
                    f.completeExceptionally(ex);
                }
            });
            long firstIndex = f.get();
            // wait every server has applied the first item
            waitFirstItemApplied(s1, firstIndex);
            waitFirstItemApplied(s2, firstIndex);
            waitFirstItemApplied(s3, firstIndex);
        }

        private void waitFirstItemApplied(ServerInfo si, long index) {
            WaitUtil.waitUtil(() -> {
                RaftGroupImpl g = (RaftGroupImpl) si.raftServer.getRaftGroup(groupId);
                return g.groupComponents.raftStatus.getShareStatus().lastApplied >= index;
            });
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
        mockSyncExCount = null;
        mockQueryStatusExCount = null;
        client = null;
        manager = null;
    }

    private class MockClientWatchManager extends ClientWatchManager {

        protected MockClientWatchManager(KvClient kvClient, Supplier<Boolean> stopped, long heartbeatIntervalMillis) {
            super(kvClient, stopped, heartbeatIntervalMillis);
        }

        @Override
        protected void sendSyncReq(RaftNode n, WatchReq req, RpcCallback<Void> c) {
            if (mockSyncExCount == null || mockSyncExCount.getAndDecrement() <= 0) {
                super.sendSyncReq(n, req, c);
            } else {
                c.call(null, new MockRuntimeException());
            }
        }

        @Override
        protected void sendQueryStatusReq(RaftNode n, int groupId, RpcCallback<KvStatusResp> c) {
            if (mockQueryStatusExCount == null || mockQueryStatusExCount.getAndDecrement() <= 0) {
                super.sendQueryStatusReq(n, groupId, c);
            } else {
                c.call(null, new MockRuntimeException());
            }
        }

        @Override
        public WritePacket processNotify(WatchNotifyReq req, SocketAddress remote) {
            return super.processNotify(req, remote);
        }
    }

    private void init(long heartbeatIntervalMillis, boolean setListener) {
        client = new KvClient() {
            @Override
            protected ClientWatchManager createClientWatchManager() {
                return new MockClientWatchManager(this, () -> getStatus() >= STATUS_PREPARE_STOP,
                        Tick.tick(heartbeatIntervalMillis));
            }
        };
        client.start();
        client.getRaftClient().clientAddNode("1,127.0.0.1:5001;2,127.0.0.1:5002;3,127.0.0.1:5003");
        client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1, 2, 3});
        manager = (MockClientWatchManager) client.getClientWatchManager();
        if (setListener) {
            manager.setListener(this, MockExecutors.ioExecutor());
        }
    }

    @AfterEach
    public void teardown() {
        client.stop(new DtTime(1, TimeUnit.SECONDS), true);
    }

    private static class PushEvent {
        final long raftIndex;
        final String key;
        final String value;

        private PushEvent(long raftIndex, String key, String value) {
            this.raftIndex = raftIndex;
            this.key = key;
            this.value = value;
        }
    }

    private void waitForEvents(PushEvent... expectEvents) {
        ArrayList<PushEvent> expectEventsList = new ArrayList<>();
        for (PushEvent e : expectEvents) {
            if (e != null) {
                expectEventsList.add(e);
            }
        }
        WaitUtil.waitUtil(() -> {
            if (events.isEmpty()) {
                return false;
            }
            WatchEvent e = events.poll();
            return checkEvent(expectEventsList, e);
        });
    }

    private Boolean checkEvent(ArrayList<PushEvent> expectEventsList, WatchEvent e) {
        for (Iterator<PushEvent> it = expectEventsList.iterator(); it.hasNext(); ) {
            PushEvent expect = it.next();
            if (expect.key.equals(e.key)) {
                String value = expect.value;
                String actualValue = e.value == null ? null : new String(e.value);
                // notice that the watch may process by a follower and it's data is not latest
                if (value == null && actualValue != null || value != null && !value.equals(actualValue)) {
                    System.out.println("got event value not match, expect: " + value + ", actual: " + actualValue);
                    return false;
                }
                if (expect.raftIndex > 0) {
                    assertEquals(expect.raftIndex, e.raftIndex);
                }
                it.remove();
                return expectEventsList.isEmpty();
            }
        }
        throw new AssertionError("unexpected event: " + e.key);
    }

    @Test
    public void testInvalidParams() {
        init(1000, true);
        assertThrows(RaftException.class, () -> manager.addWatch(groupId + 100000, "key1"));
        assertThrows(IllegalArgumentException.class, () -> manager.addWatch(groupId, ""));
        assertThrows(IllegalArgumentException.class, () -> manager.addWatch(groupId, ".key1"));
        assertThrows(IllegalArgumentException.class, () -> manager.addWatch(groupId, "key1."));
        assertThrows(IllegalArgumentException.class, () -> manager.addWatch(groupId, "key1..key2"));
    }

    @Test
    public void testAddRemoveWatch() {
        init(1000, true);
        String key1 = "testAddRemoveWatch_key1";
        String key2 = "testAddRemoveWatch_key2";
        long idx1 = client.put(groupId, key1.getBytes(), "value1".getBytes());
        long idx2 = client.put(groupId, key2.getBytes(), "value2".getBytes());
        manager.addWatch(groupId, key1, key2);
        waitForEvents(new PushEvent(idx1, key1, "value1"), new PushEvent(idx2, key2, "value2"));

        // key1 is readd
        manager.addWatch(groupId, key1);
        idx1 = client.put(groupId, key1.getBytes(), "value1_2".getBytes());
        idx2 = client.put(groupId, key2.getBytes(), "value2_2".getBytes());
        waitForEvents(new PushEvent(idx1, key1, "value1_2"));
        waitForEvents(new PushEvent(idx2, key2, "value2_2"));

        manager.removeWatch(groupId, key1);
        client.put(groupId, key1.getBytes(), "value1_3".getBytes());
        idx2 = client.put(groupId, key2.getBytes(), "value2_3".getBytes());
        waitForEvents(new PushEvent(idx2, key2, "value2_3"));

        manager.removeWatch(groupId, key1, key2);
        client.put(groupId, key1.getBytes(), "value1_4".getBytes());
        client.put(groupId, key2.getBytes(), "value2_4".getBytes());
        assertEquals(0, events.size());

        manager.addWatch(groupId, "key3");
        waitForEvents(new PushEvent(-1, "key3", null));
    }

    private void waitForEventsByUserPull(PushEvent... expectEvents) {
        ArrayList<PushEvent> expectEventsList = new ArrayList<>();
        for (PushEvent e : expectEvents) {
            if (e != null) {
                expectEventsList.add(e);
            }
        }
        WaitUtil.waitUtil(() -> {
            WatchEvent e = manager.takeEvent();
            if (e == null) {
                return false;
            }
            return checkEvent(expectEventsList, e);
        });
    }

    @Test
    public void testUserPullEvents() {
        init(1000, false);
        String key1 = "testUserPullEvents_key1";
        String key2 = "testUserPullEvents_key2";
        manager.removeListener();
        long idx1 = client.put(groupId, key1.getBytes(), "value1".getBytes());
        long idx2 = client.put(groupId, key2.getBytes(), "value2".getBytes());
        manager.addWatch(groupId, key1, key2);
        waitForEventsByUserPull(new PushEvent(idx1, key1, "value1"), new PushEvent(idx2, key2, "value2"));
    }

    @Test
    public void testEx1() {
        init(10, true);
        String key1 = "testEx1_key1";
        mockQueryStatusExCount = new AtomicInteger(3);
        manager.addWatch(groupId, key1);
        waitForEvents(new PushEvent(-1, key1, null));
    }

    @Test
    public void testEx2() {
        init(10, true);
        String key1 = "testEx2_key1";
        String key2 = "testEx2_key2";

        manager.addWatch(groupId, key1);
        waitForEvents(new PushEvent(-1, key1, null));

        mockSyncExCount = new AtomicInteger(1);
        manager.addWatch(groupId, key2);
        waitForEvents(new PushEvent(-1, key2, null));

        long idx1 = client.put(groupId, key1.getBytes(), "value1".getBytes());
        long idx2 = client.put(groupId, key2.getBytes(), "value2".getBytes());
        waitForEvents(new PushEvent(idx1, key1, "value1"));
        waitForEvents(new PushEvent(idx2, key2, "value2"));
    }

    @Test
    public void testProcessNotify() {
        init(1000, false);
        String key1 = "testProcessNotify_key1";
        WatchNotifyReq req = new WatchNotifyReq(groupId, List.of(new WatchNotify(
                1, WatchEvent.STATE_VALUE_EXISTS, key1.getBytes(), "value1".getBytes())));
        WritePacket p = manager.processNotify(req, null);
        assertEquals(CmdCodes.SUCCESS, p.getRespCode());
        assertEquals(KvCodes.CODE_REMOVE_ALL_WATCH, p.getBizCode());
        assertNull(manager.takeEvent());

        manager.addWatch(groupId, key1);
        waitForEventsByUserPull(new PushEvent(-1, key1, null));

        req = new WatchNotifyReq(groupId, List.of(new WatchNotify(
                10000, WatchEvent.STATE_VALUE_EXISTS, key1.getBytes(), "value2".getBytes())));
        p = manager.processNotify(req, null);
        assertEquals(CmdCodes.SUCCESS, p.getRespCode());
        assertEquals(KvCodes.CODE_SUCCESS, p.getBizCode());
        req = new WatchNotifyReq(groupId, List.of(new WatchNotify(
                10001, WatchEvent.STATE_VALUE_EXISTS, key1.getBytes(), "value3".getBytes())));
        p = manager.processNotify(req, null);
        assertEquals(CmdCodes.SUCCESS, p.getRespCode());
        assertEquals(KvCodes.CODE_SUCCESS, p.getBizCode());

        WatchEvent event = manager.takeEvent();
        assertNotNull(event);
        assertEquals(10001, event.raftIndex);
        assertEquals(WatchEvent.STATE_VALUE_EXISTS, event.state);
        assertEquals(key1, event.key);
        assertEquals("value3", new String(event.value));

        manager.removeWatch(groupId, key1);
        req = new WatchNotifyReq(groupId, List.of(new WatchNotify(
                10002, WatchEvent.STATE_VALUE_EXISTS, key1.getBytes(), "value4".getBytes())));
        p = manager.processNotify(req, null);
        assertEquals(CmdCodes.SUCCESS, p.getRespCode());
        assertEquals(KvCodes.CODE_REMOVE_ALL_WATCH, p.getBizCode());
        event = manager.takeEvent();
        assertNull(event);
    }
}
