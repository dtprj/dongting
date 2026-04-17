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

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.TestUtil;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.NetException;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.Peer;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.test.Tick;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class RaftClientTest {

    private static final Method CONNECT_TO_LEADER_CALLBACK = initConnectToLeaderCallbackMethod();

    private TestRaftClient client;

    @AfterEach
    public void afterTest() {
        TestUtil.stop(client);
        client = null;
    }

    // ==================== Node Management (6 tests) ====================

    @Test
    public void testClientAddNode() {
        client = new TestRaftClient();
        client.start();
        client.clientAddNode("1,127.0.0.1:40001;2,127.0.0.1:40002;3,127.0.0.1:40003");

        assertNotNull(client.getNode(1));
        assertNotNull(client.getNode(2));
        assertNotNull(client.getNode(3));
        assertEquals(1, client.getNode(1).nodeId);
        assertEquals(2, client.getNode(2).nodeId);
        assertEquals(3, client.getNode(3).nodeId);
        assertNotNull(client.getNode(1).peer);
        assertNotNull(client.getNode(2).peer);
        assertNotNull(client.getNode(3).peer);
    }

    @Test
    public void testClientAddNodeDuplicateNodeId() {
        client = new TestRaftClient();
        client.start();
        client.clientAddNode("1,127.0.0.1:40001");
        assertThrows(RaftException.class, () ->
                client.clientAddNode("1,127.0.0.1:40002"));
    }

    @Test
    public void testClientAddNodeDuplicateHostPort() {
        client = new TestRaftClient();
        client.start();
        client.clientAddNode("1,127.0.0.1:40001");
        assertThrows(RaftException.class, () ->
                client.clientAddNode("2,127.0.0.1:40001"));
    }

    @Test
    public void testClientAddNodeWhenNotRunning() {
        client = new TestRaftClient();
        assertThrows(IllegalStateException.class, () ->
                client.clientAddNode("1,127.0.0.1:40001"));
    }

    @Test
    public void testClientRemoveNode() {
        client = new TestRaftClient();
        client.start();
        client.clientAddNode("1,127.0.0.1:40001;2,127.0.0.1:40002");
        assertNotNull(client.getNode(1));
        assertNotNull(client.getNode(2));

        client.clientRemoveNode(1);
        assertNull(client.getNode(1));
        assertNotNull(client.getNode(2));
    }

    @Test
    public void testClientRemoveNodeInUse() {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});

        assertThrows(RaftException.class, () -> client.clientRemoveNode(1));
    }

    // ==================== Group Management (7 tests) ====================

    @Test
    public void testClientAddOrUpdateGroupNew() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});

        GroupInfo gi = client.getGroup(1);
        assertNotNull(gi);
        assertEquals(1, gi.groupId);
        assertEquals(3, gi.servers.size());
        assertNotNull(gi.leader);
        assertEquals(2, gi.leader.nodeId);
        assertNull(gi.leaderFuture);
        assertTrue(client.queryCount >= 1);
    }

    @Test
    public void testClientAddOrUpdateGroupSameMembers() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});
        GroupInfo gi1 = client.getGroup(1);
        // first query resolves leader to node 2
        assertNotNull(gi1.leader);

        // add same members again - should be no-op
        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});
        GroupInfo gi2 = client.getGroup(1);
        // same object, no change
        assertSame(gi1, gi2);
    }

    @Test
    public void testClientAddOrUpdateGroupDifferentMembers() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});

        // check useCount
        assertEquals(1, client.getNode(1).useCount);
        assertEquals(1, client.getNode(2).useCount);
        assertEquals(1, client.getNode(3).useCount);

        // update with different members - remove node 3, add no one (just 1,2)
        client.clientAddOrUpdateGroup(1, new int[]{1, 2});

        GroupInfo gi = client.getGroup(1);
        assertEquals(2, gi.servers.size());
        assertEquals(1, client.getNode(1).useCount);
        assertEquals(1, client.getNode(2).useCount);
        assertEquals(0, client.getNode(3).useCount);
    }

    @Test
    public void testClientAddOrUpdateGroupNodeNotExist() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        assertThrows(RaftException.class, () ->
                client.clientAddOrUpdateGroup(1, new int[]{1, 2, 4}));
    }

    @Test
    public void testClientAddOrUpdateGroupEmptyServers() {
        client = new TestRaftClient();
        client.start();
        assertThrows(IllegalArgumentException.class, () ->
                client.clientAddOrUpdateGroup(1, new int[]{}));
    }

    @Test
    public void testClientAddOrUpdateGroupDuplicateServerId() {
        client = new TestRaftClient();
        client.start();
        assertThrows(IllegalArgumentException.class, () ->
                client.clientAddOrUpdateGroup(1, new int[]{1, 1}));
    }

    @Test
    public void testClientRemoveGroup() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // Use pending query so leaderFuture stays alive
        CompletableFuture<QueryStatusResp> pending = new CompletableFuture<>();
        client.queryResponses.add(pending);

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});
        assertEquals(1, client.getNode(1).useCount);
        assertEquals(1, client.getNode(2).useCount);
        assertEquals(1, client.getNode(3).useCount);

        GroupInfo gi = client.getGroup(1);
        assertNotNull(gi.leaderFuture);
        assertFalse(gi.leaderFuture.isDone());

        client.clientRemoveGroup(1);
        assertNull(client.getGroup(1));
        assertEquals(0, client.getNode(1).useCount);
        assertEquals(0, client.getNode(2).useCount);
        assertEquals(0, client.getNode(3).useCount);

        // leaderFuture should complete exceptionally
        assertTrue(gi.leaderFuture.isCompletedExceptionally());
    }

    // ==================== Leader Finding (10 tests) ====================

    @Test
    public void testFindLeaderSyncResponse() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // default queryRaftServerStatus returns leaderId=2
        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});

        GroupInfo gi = client.getGroup(1);
        assertNotNull(gi.leader);
        assertEquals(2, gi.leader.nodeId);
        assertNull(gi.leaderFuture);
        assertTrue(client.queryCount >= 1);
    }

    @Test
    public void testFindLeaderAllNodesFail() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // All queries fail
        client.queryResponses.add(CompletableFuture.failedFuture(new RaftException("fail1")));
        client.queryResponses.add(CompletableFuture.failedFuture(new RaftException("fail2")));
        client.queryResponses.add(CompletableFuture.failedFuture(new RaftException("fail3")));

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});

        // After all queries fail, the group is replaced by createByLastFindFailTime
        GroupInfo gi = client.getGroup(1);
        assertNull(gi.leader);
        assertNull(gi.leaderFuture);
        assertNotNull(gi.lastFindFailTime);
    }

    @Test
    public void testFindLeaderLeaderIdNegative() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // First returns negative leaderId, second returns valid
        QueryStatusResp resp1 = createStatusResp(1, -1);
        client.queryResponses.add(CompletableFuture.completedFuture(resp1));
        // default response (leaderId=2) will be used for subsequent queries

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});

        GroupInfo gi = client.getGroup(1);
        assertNotNull(gi.leader);
        assertEquals(2, gi.leader.nodeId);
    }

    @Test
    public void testFindLeaderLeaderIdZero() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // First returns 0 (no leader), second returns valid
        QueryStatusResp resp1 = createStatusResp(1, 0);
        client.queryResponses.add(CompletableFuture.completedFuture(resp1));

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});

        GroupInfo gi = client.getGroup(1);
        assertNotNull(gi.leader);
        assertEquals(2, gi.leader.nodeId);
    }

    @Test
    public void testFindLeaderNotInGroup() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // First returns leaderId=99 (not in group), second returns valid
        QueryStatusResp resp1 = createStatusResp(1, 99);
        client.queryResponses.add(CompletableFuture.completedFuture(resp1));

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});

        GroupInfo gi = client.getGroup(1);
        assertNotNull(gi.leader);
        assertEquals(2, gi.leader.nodeId);
    }

    @Test
    public void testFindLeaderGroupRemovedDuringFind() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // Return a pending future so leader finding is in progress
        CompletableFuture<QueryStatusResp> pending = new CompletableFuture<>();
        client.queryResponses.add(pending);

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});
        GroupInfo gi = client.getGroup(1);
        assertNotNull(gi.leaderFuture);
        assertFalse(gi.leaderFuture.isDone());

        // Remove group while find is in progress
        client.clientRemoveGroup(1);

        // Complete the pending query - this triggers processLeaderQueryResult
        // which finds the group removed
        QueryStatusResp resp = createStatusResp(1, 2);
        pending.complete(resp);

        // The original leaderFuture should complete exceptionally
        assertTrue(gi.leaderFuture.isCompletedExceptionally());
    }

    @Test
    public void testFindLeaderConnectFail() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // Use pending query to keep leaderFuture alive, don't complete it
        CompletableFuture<QueryStatusResp> pending = new CompletableFuture<>();
        client.queryResponses.add(pending);

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});
        GroupInfo firstGi = client.getGroup(1);
        assertNotNull(firstGi.leaderFuture);
        assertFalse(firstGi.leaderFuture.isDone());

        // Track when the future completes to verify new GroupInfo is published first
        AtomicReference<Boolean> newGiPublishedBeforeFuture = new AtomicReference<>(null);
        firstGi.leaderFuture.whenComplete((result, ex) -> {
            GroupInfo currentGi = client.getGroup(1);
            newGiPublishedBeforeFuture.set(currentGi != null && currentGi != firstGi);
        });

        // Directly invoke connectToLeaderCallback with failure (no real nioClient.connect)
        invokeConnectToLeaderCallback(client, firstGi, client.getNode(2), new NetException("mock connect fail"));

        assertTrue(firstGi.leaderFuture.isCompletedExceptionally());
        // Verify that a new GroupInfo was published before the future completed
        assertTrue(newGiPublishedBeforeFuture.get());
    }

    @Test
    public void testFetchLeader() throws Exception {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        setupGroupWithLeader();

        CompletableFuture<RaftNode> f = client.fetchLeader(1);
        RaftNode leader = f.get(Tick.tick(3000), TimeUnit.MILLISECONDS);
        assertNotNull(leader);
        assertEquals(2, leader.nodeId);
    }

    @Test
    public void testFetchLeaderGroupNotFound() {
        client = new TestRaftClient();
        client.start();

        CompletableFuture<RaftNode> f = client.fetchLeader(1);
        assertTrue(f.isCompletedExceptionally());
    }

    @Test
    public void testUpdateLeaderInfoAlreadyKnown() {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        setupGroupWithLeader();

        GroupInfo gi = client.getGroup(1);
        assertNotNull(gi.leader);
        int queriesBefore = client.queryCount;

        // updateLeaderInfo with force=false should return immediately since leader is known
        CompletableFuture<GroupInfo> f = client.updateLeaderInfo(1, false);
        assertTrue(f.isDone());
        assertFalse(f.isCompletedExceptionally());
        // No new queries should have been made
        assertEquals(queriesBefore, client.queryCount);
    }

    // ==================== Send Request (12 tests) ====================

    @Test
    public void testSendRequestHappyPath() {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        setupGroupWithLeader();

        MockWritePacket request = new MockWritePacket(true);
        DtTime timeout = new DtTime(Tick.tick(3000), TimeUnit.MILLISECONDS);

        AtomicReference<ReadPacket<Void>> resultRef = new AtomicReference<>();
        AtomicReference<Throwable> exRef = new AtomicReference<>();
        client.sendRequest(1, request, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                timeout, (result, ex) -> {
                    if (ex != null) {
                        exRef.set(ex);
                    } else {
                        resultRef.set(result);
                    }
                });

        assertEquals(1, client.sendCount);
        assertNotNull(client.lastSendCallback);

        // Simulate successful response
        ReadPacket<Void> resp = new ReadPacket<>();
        resp.respCode = CmdCodes.SUCCESS;
        client.completeLastSend(resp);

        assertNull(exRef.get());
        assertNotNull(resultRef.get());
    }

    @Test
    public void testSendRequestGroupNotFound() {
        client = new TestRaftClient();
        client.start();

        MockWritePacket request = new MockWritePacket(true);
        DtTime timeout = new DtTime(Tick.tick(3000), TimeUnit.MILLISECONDS);

        AtomicReference<Throwable> exRef = new AtomicReference<>();
        client.sendRequest(999, request, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                timeout, (result, ex) -> {
                    if (ex != null) {
                        exRef.set(ex);
                    }
                });

        // NoSuchGroupException is passed directly to callback
        assertNotNull(exRef.get());
        assertInstanceOf(NoSuchGroupException.class, exRef.get());
    }

    @Test
    public void testSendRequestNotRaftLeaderRedirect() {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        setupGroupWithLeader();

        // Node 3 must be connected so redirect can succeed
        client.getNode(3).peer.status = PeerStatus.connected;

        MockWritePacket request = new MockWritePacket(true);
        DtTime timeout = new DtTime(Tick.tick(3000), TimeUnit.MILLISECONDS);

        AtomicReference<Throwable> exRef = new AtomicReference<>();
        AtomicReference<ReadPacket<Void>> resultRef = new AtomicReference<>();
        client.sendRequest(1, request, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                timeout, (result, ex) -> {
                    if (ex != null) {
                        exRef.set(ex);
                    } else {
                        resultRef.set(result);
                    }
                });

        assertEquals(1, client.sendCount);

        // Fail with NOT_RAFT_LEADER + extra indicating new leader is node 3
        NetCodeException nce = new NetCodeException(CmdCodes.NOT_RAFT_LEADER, "not leader",
                "3".getBytes(StandardCharsets.UTF_8));
        client.failLastSend(nce);

        // Should have retried to node 3
        assertEquals(2, client.sendCount);

        // Complete the retry successfully
        ReadPacket<Void> resp = new ReadPacket<>();
        resp.respCode = CmdCodes.SUCCESS;
        client.completeLastSend(resp);

        assertNull(exRef.get());
        assertNotNull(resultRef.get());

        // Verify the group now has leader = node 3
        GroupInfo gi = client.getGroup(1);
        assertEquals(3, gi.leader.nodeId);
    }

    @Test
    public void testSendRequestNotRaftLeaderNoExtra() {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        setupGroupWithLeader();

        MockWritePacket request = new MockWritePacket(true);
        DtTime timeout = new DtTime(Tick.tick(3000), TimeUnit.MILLISECONDS);

        AtomicReference<Throwable> exRef = new AtomicReference<>();
        client.sendRequest(1, request, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                timeout, (result, ex) -> {
                    if (ex != null) {
                        exRef.set(ex);
                    }
                });

        assertEquals(1, client.sendCount);

        // Fail with NOT_RAFT_LEADER but no extra (no new leader info)
        NetCodeException nce = new NetCodeException(CmdCodes.NOT_RAFT_LEADER, "not leader", null);
        client.failLastSend(nce);

        // Should NOT retry
        assertEquals(1, client.sendCount);
        assertNotNull(exRef.get());
    }

    @Test
    public void testSendRequestErrorCodeRetry() {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        setupGroupWithLeader();

        MockWritePacket request = new MockWritePacket(true);
        DtTime timeout = new DtTime(Tick.tick(3000), TimeUnit.MILLISECONDS);

        AtomicReference<Throwable> exRef = new AtomicReference<>();
        AtomicReference<ReadPacket<Void>> resultRef = new AtomicReference<>();
        client.sendRequest(1, request, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                timeout, (result, ex) -> {
                    if (ex != null) {
                        exRef.set(ex);
                    } else {
                        resultRef.set(result);
                    }
                });

        assertEquals(1, client.sendCount);

        // Fail with NOT_INIT - should trigger updateLeader and retry
        NetCodeException nce = new NetCodeException(CmdCodes.NOT_INIT, "not init", null);
        client.failLastSend(nce);

        // The retry goes through updateLeader -> findLeader -> then sends again
        // Since queryRaftServerStatus returns immediately with leaderId=2,
        // the retry send should happen synchronously
        assertEquals(2, client.sendCount);

        // Complete the retry successfully
        ReadPacket<Void> resp = new ReadPacket<>();
        resp.respCode = CmdCodes.SUCCESS;
        client.completeLastSend(resp);

        assertNull(exRef.get());
        assertNotNull(resultRef.get());
    }

    @Test
    public void testSendRequestNoLeaderUpdateFirst() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // Add group but without leader - all queries return pending futures initially
        // so the leader won't be resolved immediately
        CompletableFuture<QueryStatusResp> pending1 = new CompletableFuture<>();
        client.queryResponses.add(pending1);

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});
        GroupInfo gi = client.getGroup(1);
        assertNotNull(gi.leaderFuture);
        assertFalse(gi.leaderFuture.isDone());
        assertNull(gi.leader);

        // Now send request - should wait for leader to be found
        MockWritePacket request = new MockWritePacket(true);
        DtTime timeout = new DtTime(Tick.tick(3000), TimeUnit.MILLISECONDS);

        AtomicReference<Throwable> exRef = new AtomicReference<>();
        AtomicReference<ReadPacket<Void>> resultRef = new AtomicReference<>();
        client.sendRequest(1, request, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                timeout, (result, ex) -> {
                    if (ex != null) {
                        exRef.set(ex);
                    } else {
                        resultRef.set(result);
                    }
                });

        // No send yet since leader not found
        assertEquals(0, client.sendCount);

        // Complete the query with leader info
        QueryStatusResp resp = createStatusResp(1, 2);
        pending1.complete(resp);

        // Now the send should have happened
        assertEquals(1, client.sendCount);

        // Complete the send successfully and verify callback
        ReadPacket<Void> sendResp = new ReadPacket<>();
        client.completeLastSend(sendResp);
        assertNull(exRef.get());
        assertNotNull(resultRef.get());
    }

    @Test
    public void testSendRequestCanRetryFalse() {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        setupGroupWithLeader();

        // canRetry() returns false by default, use MockWritePacket with canRetry=false
        MockWritePacket request = new MockWritePacket(false);
        DtTime timeout = new DtTime(Tick.tick(3000), TimeUnit.MILLISECONDS);

        AtomicReference<Throwable> exRef = new AtomicReference<>();
        client.sendRequest(1, request, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                timeout, (result, ex) -> {
                    if (ex != null) {
                        exRef.set(ex);
                    }
                });

        assertEquals(1, client.sendCount);

        // Fail with NOT_RAFT_LEADER + extra
        NetCodeException nce = new NetCodeException(CmdCodes.NOT_RAFT_LEADER, "not leader",
                "3".getBytes(StandardCharsets.UTF_8));
        client.failLastSend(nce);

        // Should NOT retry since canRetry() is false
        assertEquals(1, client.sendCount);
        assertNotNull(exRef.get());
    }

    @Test
    public void testSendRequestAlreadyRetried() {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        setupGroupWithLeader();

        client.getNode(3).peer.status = PeerStatus.connected;

        MockWritePacket request = new MockWritePacket(true);
        DtTime timeout = new DtTime(Tick.tick(3000), TimeUnit.MILLISECONDS);

        AtomicReference<Throwable> exRef = new AtomicReference<>();
        client.sendRequest(1, request, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                timeout, (result, ex) -> {
                    if (ex != null) {
                        exRef.set(ex);
                    }
                });

        assertEquals(1, client.sendCount);

        // First NOT_RAFT_LEADER triggers retry with retry=1
        NetCodeException nce1 = new NetCodeException(CmdCodes.NOT_RAFT_LEADER, "not leader",
                "3".getBytes(StandardCharsets.UTF_8));
        client.failLastSend(nce1);

        assertEquals(2, client.sendCount);

        // Second NOT_RAFT_LEADER - retry=1 so should NOT retry again
        NetCodeException nce2 = new NetCodeException(CmdCodes.NOT_RAFT_LEADER, "still not leader",
                "1".getBytes(StandardCharsets.UTF_8));
        client.failLastSend(nce2);

        // Should NOT retry again
        assertEquals(2, client.sendCount);
        assertNotNull(exRef.get());
    }

    @Test
    public void testSendRequestSysError() {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        setupGroupWithLeader();

        MockWritePacket request = new MockWritePacket(true);
        DtTime timeout = new DtTime(Tick.tick(3000), TimeUnit.MILLISECONDS);

        AtomicReference<Throwable> exRef = new AtomicReference<>();
        client.sendRequest(1, request, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                timeout, (result, ex) -> {
                    if (ex != null) {
                        exRef.set(ex);
                    }
                });

        GroupInfo giBefore = client.getGroup(1);
        assertNull(giBefore.lastLeaderFailTime);

        NetCodeException nce = new NetCodeException(CmdCodes.SYS_ERROR, "sys error", null);
        client.failLastSend(nce);

        assertNotNull(exRef.get());
        // lastLeaderFailTime should be set
        GroupInfo giAfter = client.getGroup(1);
        assertNotNull(giAfter.lastLeaderFailTime);
        assertEquals(1, client.sendCount);
    }

    @Test
    public void testSendRequestClientError() {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        setupGroupWithLeader();

        MockWritePacket request = new MockWritePacket(true);
        DtTime timeout = new DtTime(Tick.tick(3000), TimeUnit.MILLISECONDS);

        AtomicReference<Throwable> exRef = new AtomicReference<>();
        client.sendRequest(1, request, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                timeout, (result, ex) -> {
                    if (ex != null) {
                        exRef.set(ex);
                    }
                });

        NetCodeException nce = new NetCodeException(CmdCodes.CLIENT_ERROR, "client error", null);
        client.failLastSend(nce);

        assertNotNull(exRef.get());
        // CLIENT_ERROR should NOT set lastLeaderFailTime
        GroupInfo gi = client.getGroup(1);
        assertNull(gi.lastLeaderFailTime);
        assertEquals(1, client.sendCount);
    }

    @Test
    public void testSendRequestNonNetCodeException() {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        setupGroupWithLeader();

        MockWritePacket request = new MockWritePacket(true);
        DtTime timeout = new DtTime(Tick.tick(3000), TimeUnit.MILLISECONDS);

        AtomicReference<Throwable> exRef = new AtomicReference<>();
        client.sendRequest(1, request, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                timeout, (result, ex) -> {
                    if (ex != null) {
                        exRef.set(ex);
                    }
                });

        // Non-NetCodeException - should set lastLeaderFailTime
        RuntimeException ex = new RuntimeException("connection reset");
        client.failLastSend(ex);

        assertNotNull(exRef.get());
        GroupInfo gi = client.getGroup(1);
        assertNotNull(gi.lastLeaderFailTime);
        assertEquals(1, client.sendCount);
    }

    @Test
    public void testSendRequestStopped() {
        client = new TestRaftClient();
        client.start();
        setupNodes();
        setupGroupWithLeader();

        TestUtil.stop(client);

        MockWritePacket request = new MockWritePacket(true);
        DtTime timeout = new DtTime(Tick.tick(3000), TimeUnit.MILLISECONDS);

        assertThrows(IllegalStateException.class, () ->
                client.sendRequest(1, request, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                        timeout, (result, ex) -> {}));
    }

    // ==================== Additional Scenarios (5 tests) ====================

    @Test
    public void testFindLeaderFastFail() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // All queries fail
        client.queryResponses.add(CompletableFuture.failedFuture(new RaftException("fail1")));
        client.queryResponses.add(CompletableFuture.failedFuture(new RaftException("fail2")));
        client.queryResponses.add(CompletableFuture.failedFuture(new RaftException("fail3")));

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});
        GroupInfo gi = client.getGroup(1);
        assertNull(gi.leader);
        assertNull(gi.leaderFuture);
        assertNotNull(gi.lastFindFailTime);

        // Try again quickly - should fast fail due to lastFindFailTime
        CompletableFuture<GroupInfo> f2 = client.updateLeaderInfo(1, false);
        assertTrue(f2.isCompletedExceptionally());
    }

    @Test
    public void testGroupUpdateReuseOldLeader() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // Add group, leader resolves to node 2
        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});
        GroupInfo gi1 = client.getGroup(1);
        assertNotNull(gi1.leader);
        assertEquals(2, gi1.leader.nodeId);
        assertNull(gi1.leaderFuture);

        // Update with different members that still include node 2
        // The old leader (node 2) should be reused
        client.clientAddOrUpdateGroup(1, new int[]{2, 3});
        GroupInfo gi2 = client.getGroup(1);
        assertNotNull(gi2.leader);
        assertEquals(2, gi2.leader.nodeId);
        // Should not create a new leaderFuture since old leader is still valid
        assertNull(gi2.leaderFuture);
    }

    @Test
    public void testUpdateLeaderInfoReuseFuture() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // Use pending futures so leader is not resolved immediately
        CompletableFuture<QueryStatusResp> pending1 = new CompletableFuture<>();
        client.queryResponses.add(pending1);

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});
        GroupInfo gi = client.getGroup(1);
        assertNotNull(gi.leaderFuture);
        assertFalse(gi.leaderFuture.isDone());

        int queriesBefore = client.queryCount;

        // Call updateLeaderInfo again - should reuse the existing future
        CompletableFuture<GroupInfo> f = client.updateLeaderInfo(1, false);
        assertSame(gi.leaderFuture, f);

        // No new queries yet (the first is still pending)
        assertEquals(queriesBefore, client.queryCount);

        // Complete the pending query
        QueryStatusResp resp = createStatusResp(1, 2);
        pending1.complete(resp);

        assertTrue(f.isDone());
    }

    @Test
    public void testConnectToLeaderSuccess() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // Use pending future for query, don't complete it to avoid real nioClient.connect()
        CompletableFuture<QueryStatusResp> pending = new CompletableFuture<>();
        client.queryResponses.add(pending);

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});
        GroupInfo gi = client.getGroup(1);
        assertNotNull(gi.leaderFuture);
        assertFalse(gi.leaderFuture.isDone());

        // Set peer connected and invoke connectToLeaderCallback with success
        client.getNode(2).peer.status = PeerStatus.connected;
        invokeConnectToLeaderCallback(client, gi, client.getNode(2), null);

        assertTrue(gi.leaderFuture.isDone());
        assertFalse(gi.leaderFuture.isCompletedExceptionally());
        GroupInfo resultGi = gi.leaderFuture.join();
        assertEquals(2, resultGi.leader.nodeId);
    }

    @Test
    public void testFindLeaderQueryThrowException() {
        client = new TestRaftClient();
        client.start();
        setupNodes();

        // First query throws synchronously, second returns valid result
        client.queryResponses.add(null); // marker to throw
        // default response returns leaderId=2

        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});

        GroupInfo gi = client.getGroup(1);
        // Should eventually find leader despite the synchronous exception
        assertNotNull(gi.leader);
        assertEquals(2, gi.leader.nodeId);
    }

    // ==================== Helper Methods ====================

    private void setupNodes() {
        client.clientAddNode("1,127.0.0.1:40001;2,127.0.0.1:40002;3,127.0.0.1:40003");
        client.getNode(1).peer.status = PeerStatus.connected;
        client.getNode(2).peer.status = PeerStatus.connected;
        client.getNode(3).peer.status = PeerStatus.connected;
    }

    private void setupGroupWithLeader() {
        // queryRaftServerStatus default returns leaderId=2
        client.clientAddOrUpdateGroup(1, new int[]{1, 2, 3});
    }

    private static QueryStatusResp createStatusResp(int groupId, int leaderId) {
        QueryStatusResp resp = new QueryStatusResp();
        resp.groupId = groupId;
        resp.leaderId = leaderId;
        resp.setFlag(true, false, true, false);
        return resp;
    }

    private static Method initConnectToLeaderCallbackMethod() {
        try {
            Method method = RaftClient.class.getDeclaredMethod(
                    "connectToLeaderCallback", GroupInfo.class, RaftNode.class, Throwable.class);
            method.setAccessible(true);
            return method;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void invokeConnectToLeaderCallback(RaftClient client, GroupInfo gi,
                                                       RaftNode leader, Throwable ex) {
        try {
            CONNECT_TO_LEADER_CALLBACK.invoke(client, gi, leader, ex);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    // ==================== TestRaftClient ====================

    private static class TestRaftClient extends RaftClient {
        List<CompletableFuture<QueryStatusResp>> queryResponses = new ArrayList<>();
        int queryCount;
        RpcCallback<?> lastSendCallback;
        int sendCount;

        private TestRaftClient() {
            super(new RaftClientConfig(), createNioClientConfig());
            // disable biz executor so callbacks run in current thread
            getConfig().useBizExecutor = false;
        }

        private static NioClientConfig createNioClientConfig() {
            NioClientConfig config = new NioClientConfig("TestRaftClient");
            // Set to 0 to make acquirePermit return false immediately (no blocking)
            config.maxOutRequests = 0;
            config.maxOutBytes = 0;
            return config;
        }

        @Override
        protected CompletableFuture<QueryStatusResp> queryRaftServerStatus(int nodeId, int groupId) {
            queryCount++;
            int idx = queryCount - 1;
            if (idx < queryResponses.size()) {
                CompletableFuture<QueryStatusResp> resp = queryResponses.get(idx);
                if (resp == null) {
                    // null marker means throw synchronous exception
                    throw new RaftException("sync query error for node " + nodeId);
                }
                return resp;
            }
            // Default: return leaderId=2
            return CompletableFuture.completedFuture(createStatusResp(groupId, 2));
        }

        @Override
        protected <T> void sendRpcToPeer(Peer peer, WritePacket request,
                                          DecoderCallbackCreator<T> decoder,
                                          DtTime timeout, RpcCallback<T> callback) {
            sendCount++;
            lastSendCallback = callback;
        }

        @SuppressWarnings("unchecked")
        <T> void completeLastSend(ReadPacket<T> result) {
            ((RpcCallback<T>) lastSendCallback).call(result, null);
        }

        <T> void failLastSend(Throwable ex) {
            lastSendCallback.call(null, ex);
        }
    }

    // ==================== MockWritePacket ====================

    private static class MockWritePacket extends WritePacket {
        private final boolean retryable;

        MockWritePacket(boolean retryable) {
            this.retryable = retryable;
        }

        @Override
        protected int calcActualBodySize() {
            return 0;
        }

        @Override
        protected boolean encodeBody(com.github.dtprj.dongting.codec.EncodeContext context, ByteBuffer dest) {
            return true;
        }

        @Override
        public boolean canRetry() {
            return retryable;
        }
    }
}
