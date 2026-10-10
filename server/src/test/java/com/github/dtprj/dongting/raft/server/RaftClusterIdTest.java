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
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.Peer;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.rpc.AppendReqWritePacket;
import com.github.dtprj.dongting.raft.rpc.AppendResp;
import com.github.dtprj.dongting.raft.rpc.InstallSnapshotReq;
import com.github.dtprj.dongting.raft.rpc.TransferLeaderReq;
import com.github.dtprj.dongting.raft.rpc.VoteReq;
import com.github.dtprj.dongting.raft.rpc.VoteResp;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class RaftClusterIdTest extends ServerTestBase {

    private static final int WRONG_CLUSTER_ID = 0x7F123456;

    public RaftClusterIdTest() {
        super(false);
    }

    @Test
    void testRaftClusterId() throws Exception {
        ServerInfo[] sis = new ServerInfo[3];
        try {
            String servers = "1,127.0.0.1:14401;2,127.0.0.1:14402;3,127.0.0.1:14403";
            String members = "1,2,3";
            for (int i = 0; i < 3; i++) {
                sis[i] = createServer(i + 1, servers, members, "");
            }
            for (ServerInfo si : sis) {
                waitStart(si);
            }
            ServerInfo leader = waitLeaderElectAndGetLeaderId(groupId, sis);

            // each node has a non-zero cluster id, and the raft log is not empty
            // (the leader writes heartbeat log entries), so the cluster id check is active
            for (ServerInfo si : sis) {
                WaitUtil.waitUtil(() -> si.gc.raftStatus.raftClusterId != 0
                        && si.gc.raftStatus.lastLogIndex >= 1, si.gc.fiberGroup.getExecutor());
            }
            // all nodes converge to the same cluster id
            int clusterId = getStatus(sis[0], rs -> rs.raftClusterId);
            assertTrue(clusterId != 0);
            assertEquals(clusterId, (int) getStatus(sis[1], rs -> rs.raftClusterId));
            assertEquals(clusterId, (int) getStatus(sis[2], rs -> rs.raftClusterId));

            ServerInfo follower = sis[0].nodeId == leader.nodeId ? sis[1] : sis[0];
            NioClient client = new NioClient(new NioClientConfig());
            client.start();
            try {
                Peer followerPeer = client.addPeer(new HostPort("127.0.0.1", 14400 + follower.nodeId))
                        .get(5, TimeUnit.SECONDS);
                int term = getStatus(follower, rs -> rs.currentTerm);
                long lastLogIndex = getStatus(follower, rs -> rs.lastLogIndex);
                int lastLogTerm = getStatus(follower, rs -> rs.lastLogTerm);

                // vote with a wrong cluster id and a huge term is rejected,
                // and the rejection happens before term update
                assertRejected(() -> sendVote(client, followerPeer, WRONG_CLUSTER_ID, term + 100,
                        lastLogIndex + 10, Integer.MAX_VALUE));
                assertEquals(term, (int) getStatus(follower, rs -> rs.currentTerm));
                // vote without cluster id is also rejected
                assertRejected(() -> sendVote(client, followerPeer, 0, term + 100,
                        lastLogIndex + 10, Integer.MAX_VALUE));
                assertEquals(term, (int) getStatus(follower, rs -> rs.currentTerm));

                // append with a wrong cluster id and a huge term is rejected
                assertRejected(() -> sendAppend(client, followerPeer, WRONG_CLUSTER_ID, term + 100));
                assertEquals(term, (int) getStatus(follower, rs -> rs.currentTerm));

                // install snapshot with a wrong cluster id is rejected before any state change
                assertRejected(() -> sendInstall(client, followerPeer, WRONG_CLUSTER_ID, term + 100, lastLogIndex));
                assertEquals(term, (int) getStatus(follower, rs -> rs.currentTerm));
                assertFalse((boolean) getStatus(follower, rs -> rs.installSnapshot));
                // the leader appends heartbeat log entries at any time, so lastLogIndex may grow;
                // it would be lastLogIndex + 100 (lastIncludedIndex) if the install took effect
                assertTrue(getStatus(follower, rs -> rs.lastLogIndex) < lastLogIndex + 100);

                // transfer leader with a wrong cluster id is rejected
                assertRejected(() -> sendTransfer(client, followerPeer, WRONG_CLUSTER_ID, follower.nodeId));
                assertEquals(term, (int) getStatus(follower, rs -> rs.currentTerm));

                // control: vote with the correct cluster id is processed normally, term is updated
                VoteResp voteResp = sendVote(client, followerPeer, clusterId, term + 100, lastLogIndex, lastLogTerm);
                assertEquals(term + 100, voteResp.term);
            } finally {
                client.stop(new DtTime(2, TimeUnit.SECONDS));
            }
        } finally {
            for (ServerInfo si : sis) {
                waitStop(si);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"305419896", "0"})
    void testRecoveryStateCheck(int initId) throws Exception {
        // a node restoring from an interrupted install snapshot has data (installSnapshot flag set,
        // commitIndex >= 1) but lastLogIndex == 0; the cluster id check must still be active.
        // a data node without cluster id (initId = 0, e.g. the status file is lost) also rejects
        // requests carrying a non-zero id, it never binds itself to an unknown lineage
        initSnapshot = true;
        initCommitIndex = 100;
        initRaftClusterId = initId;
        ServerInfo si = null;
        NioClient client = new NioClient(new NioClientConfig());
        try {
            // single-member group; the vote fiber skips while installSnapshot is true, so it never elects
            si = createServer(1, "1,127.0.0.1:14401", "1", "");
            ServerInfo finalSi = si;
            WaitUtil.waitUtil(() -> finalSi.gc.raftStatus.isInitFinished(), si.gc.fiberGroup.getExecutor());
            assertFalse((boolean) getStatus(si, rs -> rs.initFailed));
            assertEquals(initId, (int) getStatus(si, rs -> rs.raftClusterId));
            // lastLogIndex is 0 in this state, exactly the case that lastLogIndex-only check misses
            assertEquals(0L, (long) getStatus(si, rs -> rs.lastLogIndex));

            client.start();
            Peer peer = client.addPeer(new HostPort("127.0.0.1", 14401)).get(5, TimeUnit.SECONDS);
            int term = getStatus(si, rs -> rs.currentTerm);

            assertRejected(() -> sendInstall(client, peer, WRONG_CLUSTER_ID, term + 100, 0));
            assertEquals(term, (int) getStatus(si, rs -> rs.currentTerm));
            assertTrue((boolean) getStatus(si, rs -> rs.installSnapshot));
            assertEquals(0L, (long) getStatus(si, rs -> rs.lastLogIndex));

            assertRejected(() -> sendVote(client, peer, WRONG_CLUSTER_ID, term + 100, 10, Integer.MAX_VALUE));
            assertEquals(term, (int) getStatus(si, rs -> rs.currentTerm));
        } finally {
            client.stop(new DtTime(2, TimeUnit.SECONDS));
            waitStop(si);
        }
    }

    private <T> T getStatus(ServerInfo si, Function<RaftStatusImpl, T> fn) throws Exception {
        CompletableFuture<T> f = new CompletableFuture<>();
        si.gc.fiberGroup.getExecutor().execute(() -> {
            try {
                f.complete(fn.apply(si.gc.raftStatus));
            } catch (Throwable e) {
                f.completeExceptionally(e);
            }
        });
        return f.get(5, TimeUnit.SECONDS);
    }

    private interface RpcCall<T> {
        T call() throws Exception;
    }

    private <T> void assertRejected(RpcCall<T> call) throws Exception {
        ExecutionException e = assertThrows(ExecutionException.class, call::call);
        NetCodeException ne = assertInstanceOf(NetCodeException.class, e.getCause());
        assertEquals(CmdCodes.CLIENT_ERROR, ne.getCode());
        assertTrue(ne.getMessage().contains("raft cluster id not match"));
    }

    private VoteResp sendVote(NioClient client, Peer peer, int clusterId, int term,
                              long lastLogIndex, int lastLogTerm) throws Exception {
        VoteReq req = new VoteReq();
        req.groupId = groupId;
        req.term = term;
        req.candidateId = 1;
        req.raftClusterId = clusterId;
        req.lastLogIndex = lastLogIndex;
        req.lastLogTerm = lastLogTerm;
        req.lastConfigChangeIndex = Long.MAX_VALUE;
        SimpleWritePacket wf = new SimpleWritePacket(req);
        wf.command = Commands.RAFT_REQUEST_VOTE;
        return sendRpc(client, peer, wf, ctx -> ctx.toDecoderCallback(new VoteResp.Callback()));
    }

    private AppendResp sendAppend(NioClient client, Peer peer, int clusterId, int term) throws Exception {
        AppendReqWritePacket wf = new AppendReqWritePacket();
        wf.command = Commands.RAFT_APPEND_ENTRIES;
        wf.groupId = groupId;
        wf.term = term;
        wf.leaderId = 1;
        wf.prevLogIndex = 0;
        wf.prevLogTerm = 0;
        wf.leaderCommit = 0;
        wf.raftClusterId = clusterId;
        wf.logs = Collections.emptyList();
        return sendRpc(client, peer, wf, ctx -> ctx.toDecoderCallback(new AppendResp.Callback()));
    }

    private AppendResp sendInstall(NioClient client, Peer peer, int clusterId, int term,
                                   long lastLogIndex) throws Exception {
        InstallSnapshotReq req = new InstallSnapshotReq();
        req.groupId = groupId;
        req.term = term;
        req.leaderId = 1;
        req.lastIncludedIndex = lastLogIndex + 100;
        req.lastIncludedTerm = 1;
        req.offset = 0;
        req.done = false;
        req.members = new HashSet<>();
        req.members.add(1);
        req.raftClusterId = clusterId;
        InstallSnapshotReq.InstallReqWritePacket wf = new InstallSnapshotReq.InstallReqWritePacket(req);
        wf.command = Commands.RAFT_INSTALL_SNAPSHOT;
        return sendRpc(client, peer, wf, ctx -> ctx.toDecoderCallback(new AppendResp.Callback()));
    }

    private Void sendTransfer(NioClient client, Peer peer, int clusterId, int newLeaderId) throws Exception {
        TransferLeaderReq req = new TransferLeaderReq();
        req.groupId = groupId;
        req.term = 0;
        req.oldLeaderId = 1;
        req.newLeaderId = newLeaderId;
        req.raftClusterId = clusterId;
        SimpleWritePacket wf = new SimpleWritePacket(req);
        wf.command = Commands.RAFT_TRANSFER_LEADER;
        return sendRpc(client, peer, wf, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR);
    }

    private <T> T sendRpc(NioClient client, Peer peer, WritePacket wf,
                          DecoderCallbackCreator<T> respDecoderCallback) throws Exception {
        CompletableFuture<T> f = new CompletableFuture<>();
        client.sendRequest(peer, wf, respDecoderCallback, new DtTime(5, TimeUnit.SECONDS), (rf, ex) -> {
            if (ex != null) {
                f.completeExceptionally(ex);
            } else {
                f.complete(rf.getBody());
            }
        });
        return f.get(5, TimeUnit.SECONDS);
    }
}
