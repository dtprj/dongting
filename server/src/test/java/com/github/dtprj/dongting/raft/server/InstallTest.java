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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class InstallTest extends ServerTestBase {

    @BeforeEach
    public void beforeEach() {
        this.idxCacheSize = 4;
        this.idxFlushThreshold = 2;
        this.idxItemsPerFile = 8;
        this.logFileSize = 1024;
        this.electTimeout = 50;
    }

    @Override
    protected void config(RaftGroupConfig config) {
        config.autoDeleteLogDelayMillis = 0;
        config.maxKeepSnapshots = 1;
        config.saveSnapshotWhenClose = false;
    }

    @Test
    void testNewEmptyFollowerAddToGroup() throws Exception {
        testNewEmptyFollowerAddToGroup(1, 200);
        dirMap.clear();
        testNewEmptyFollowerAddToGroup(2, 400);
        dirMap.clear();
        testNewEmptyFollowerAddToGroup(3, 400);
        dirMap.clear();
        testNewEmptyFollowerAddToGroup(4, 400);
        dirMap.clear();
        testNewEmptyFollowerAddToGroup(5, 400);
    }

    private void testNewEmptyFollowerAddToGroup(int count, int bodySize) throws Exception {
        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003";
        String members = "1,2,3";
        String observers = "";
        ServerInfo s1 = createServer(1, servers, members, observers);
        ServerInfo s2 = createServer(2, servers, members, observers);

        waitStart(s1);
        waitStart(s2);
        ServerInfo leader = waitLeaderElectAndGetLeaderId(groupId, s1, s2);

        KvClient client = new KvClient();
        client.start();
        client.getRaftClient().clientAddNode("1,127.0.0.1:5001;2,127.0.0.1:5002;3,127.0.0.1:5003");
        client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1, 2, 3});

        HashMap<String, byte[]> expectMap = new HashMap<>();
        long raftIndex1 = putValues(groupId, client, "beforeInstallKey", count, bodySize, expectMap);

        // transfer leader make nextIndex to lastLogIndex + 1, then trigger install
        DtTime timeout = new DtTime(5, TimeUnit.SECONDS);
        AdminRaftClient adminClient = new AdminRaftClient();
        adminClient.start();
        adminClient.clientAddNode(servers);
        adminClient.clientAddOrUpdateGroup(groupId, new int[]{1, 2, 3});
        adminClient.transferLeader(groupId, leader.nodeId, leader.nodeId == 1 ? 2 : 1, timeout).get(5, TimeUnit.SECONDS);
        leader = leader == s1 ? s2 : s1;

        // start server 3
        ServerInfo s3 = createServer(3, servers, members, observers);
        waitStart(s3);

        // wait server 3 install snapshot and catch up
        RaftGroupImpl g3 = (RaftGroupImpl) s3.raftServer.getRaftGroup(1);
        TestUtil.waitUtil(() -> g3.groupComponents.raftStatus.getShareStatus().lastApplied >= raftIndex1);

        // put after install
        long raftIndex2 = putValues(groupId, client, "afterInstallKey", count, bodySize, expectMap);
        TestUtil.waitUtil(() -> g3.groupComponents.raftStatus.getShareStatus().lastApplied >= raftIndex2);

        // transfer leader to server 3
        adminClient.transferLeader(groupId, leader.nodeId, 3, timeout).get(5, TimeUnit.SECONDS);

        // check data in server 3
        check(groupId, client, expectMap);

        // restart to check restore after install snapshot
        waitStop(s1);
        waitStop(s2);
        waitStop(s3);
        s1 = createServer(1, servers, members, observers);
        s2 = createServer(2, servers, members, observers);
        s3 = createServer(3, servers, members, observers);
        waitStart(s1);
        waitStart(s2);
        waitStart(s3);
        leader = waitLeaderElectAndGetLeaderId(groupId, s1, s2, s3);

        if (leader.nodeId != 3) {
            adminClient.transferLeader(groupId, leader.nodeId, 3, timeout).get(5, TimeUnit.SECONDS);
        }
        check(groupId, client, expectMap);

        // stop all
        waitStop(s1);
        waitStop(s2);
        waitStop(s3);
        client.stop(timeout);
        adminClient.stop(timeout);
    }

    static long putValues(int groupId, KvClient client, String keyPrefix, int count, int bodySize,
                           HashMap<String, byte[]> expectMap) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(count);
        AtomicLong lastRaftIndex = new AtomicLong(0);
        Random r = new Random();
        for (int keyIndex = 0; keyIndex < count; keyIndex++) {
            String key = keyPrefix + keyIndex;
            byte[] body = new byte[bodySize];
            r.nextBytes(body);
            expectMap.put(key, body);
            client.put(groupId, key.getBytes(), body, (raftIndex, ex) -> {
                lastRaftIndex.set(raftIndex);
                latch.countDown();
            });
        }
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        return lastRaftIndex.get();
    }


    static void check(int groupId, KvClient client, HashMap<String, byte[]> expectMap) {
        HashMap<String, byte[]> actualMap = new HashMap<>();
        List<byte[]> keys = expectMap.keySet().stream().map(String::getBytes).collect(Collectors.toList());
        List<KvNode> list = client.batchGet(groupId, keys);
        for (int i = 0; i < list.size(); i++) {
            actualMap.put(new String(keys.get(i)), list.get(i).data);
        }
        for (String k : expectMap.keySet()) {
            byte[] expect = expectMap.get(k);
            byte[] actual = actualMap.get(k);
            assertArrayEquals(expect, actual);
        }
    }

    @Test
    void testTruncateAndInstall() throws Exception {
        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003";
        String members = "1,2,3";
        String observers = "";
        ServerInfo s1 = createServer(1, servers, members, observers);
        ServerInfo s2 = createServer(2, servers, members, observers);
        ServerInfo s3 = createServer(3, servers, members, observers);

        waitStart(s1);
        waitStart(s2);
        waitStart(s3);
        ServerInfo leader = waitLeaderElectAndGetLeaderId(groupId, s1, s2, s3);

        KvClient client = new KvClient();
        client.start();
        client.getRaftClient().clientAddNode("1,127.0.0.1:5001;2,127.0.0.1:5002;3,127.0.0.1:5003");
        client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1, 2, 3});

        AdminRaftClient adminClient = new AdminRaftClient();
        adminClient.start();
        adminClient.clientAddNode(servers);
        adminClient.clientAddOrUpdateGroup(groupId, new int[]{1, 2, 3});

        DtTime timeout = new DtTime(5, TimeUnit.SECONDS);
        if (leader.nodeId == 3) {
            adminClient.transferLeader(groupId, leader.nodeId, 1, timeout).get(5, TimeUnit.SECONDS);
            leader = s1;
        }

        HashMap<String, byte[]> expectMap = new HashMap<>();
        long raftIndex1 = putValues(groupId, client, "before", 4, 400, expectMap);

        // wait server 3 catch up
        RaftGroupImpl g3 = (RaftGroupImpl) s3.raftServer.getRaftGroup(groupId);
        TestUtil.waitUtil(() -> g3.groupComponents.raftStatus.getShareStatus().lastApplied >= raftIndex1);

        waitStop(s3);

        long raftIndex2 = putValues(groupId, client, "after", 4,400, expectMap);

        // save snapshot and truncate logs, since
        // config.autoDeleteLogDelayMillis = 0 and config.maxKeepSnapshots = 1
        leader.group.fireSaveSnapshot().get(5, TimeUnit.SECONDS);

        // restart server 3
        s3 = createServer(3, servers, members, observers);
        waitStart(s3);

        // wait server 3 install snapshot and catch up
        RaftGroupImpl g3New = (RaftGroupImpl) s3.raftServer.getRaftGroup(groupId);
        TestUtil.waitUtil(() -> g3New.groupComponents.raftStatus.getShareStatus().lastApplied >= raftIndex2);

        adminClient.transferLeader(groupId, leader.nodeId, 3, timeout).get(5, TimeUnit.SECONDS);
        check(groupId, client, expectMap);

        // stop all
        waitStop(s1);
        waitStop(s2);
        waitStop(s3);
        client.stop(timeout);
        adminClient.stop(timeout);
    }
}
