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
package com.github.dtprj.dongting.raft.admin;

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.codec.PbLongCallback;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyReqPacket;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.PbIntWritePacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.raft.RaftClient;
import com.github.dtprj.dongting.raft.RaftClientConfig;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.rpc.AdminAddGroupReq;
import com.github.dtprj.dongting.raft.rpc.AdminAddNodeReq;
import com.github.dtprj.dongting.raft.rpc.AdminCommitOrAbortReq;
import com.github.dtprj.dongting.raft.rpc.AdminListGroupsResp;
import com.github.dtprj.dongting.raft.rpc.AdminListNodesResp;
import com.github.dtprj.dongting.raft.rpc.AdminPrepareConfigChangeReq;
import com.github.dtprj.dongting.raft.rpc.TransferLeaderReq;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class AdminRaftClient extends RaftClient {

    public AdminRaftClient() {
        this(new RaftClientConfig(), new NioClientConfig());
    }

    public AdminRaftClient(RaftClientConfig raftClientConfig, NioClientConfig nioClientConfig) {
        super(raftClientConfig, nioClientConfig);
    }

    public CompletableFuture<QueryStatusResp> queryRaftServerStatus(int nodeId, int groupId) {
        return super.queryRaftServerStatus(nodeId, groupId);
    }

    public CompletableFuture<Void> transferLeader(int groupId, int oldLeader, int newLeader, DtTime timeout) {
        if (oldLeader == newLeader) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "old and new leader id equals: " + oldLeader));
        }
        return updateLeaderInfo(groupId, true).thenCompose(leaderGroup -> {
            if (leaderGroup.leader.nodeId != oldLeader) {
                throw new RaftException("old leader not match");
            }
            boolean foundNewLeader = false;
            for (RaftNode n : leaderGroup.servers) {
                if (n.nodeId == newLeader) {
                    foundNewLeader = true;
                    break;
                }
            }
            if (!foundNewLeader) {
                throw new RaftException("new leader not found is servers list: " + newLeader);
            }
            TransferLeaderReq req = new TransferLeaderReq();
            req.groupId = leaderGroup.groupId;
            req.oldLeaderId = oldLeader;
            req.newLeaderId = newLeader;
            SimpleWritePacket p = new SimpleWritePacket(req);
            p.command = Commands.RAFT_ADMIN_TRANSFER_LEADER;
            DecoderCallbackCreator<Void> dc = DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR;
            CompletableFuture<ReadPacket<Void>> f = new CompletableFuture<>();
            nioClient.sendRequest(leaderGroup.leader.peer, p, dc, timeout, RpcCallback.fromFuture(f));
            return f.thenApply(rp -> null);
        });
    }

    public CompletableFuture<Long> prepareConfigChange(int groupId, Set<Integer> members, Set<Integer> observers,
                                                       Set<Integer> prepareMembers, Set<Integer> prepareObservers,
                                                       DtTime timeout) {
        AdminPrepareConfigChangeReq req = new AdminPrepareConfigChangeReq();
        req.groupId = groupId;
        req.members = new HashSet<>(members);
        req.observers = new HashSet<>(observers);
        req.preparedMembers = new HashSet<>(prepareMembers);
        req.preparedObservers = new HashSet<>(prepareObservers);
        SimpleWritePacket p = new SimpleWritePacket(Commands.RAFT_ADMIN_PREPARE_CHANGE, req);

        DecoderCallbackCreator<Long> dc = PbLongCallback.CALLBACK_CREATOR;
        CompletableFuture<Long> r = new CompletableFuture<>();
        sendRequest(groupId, p, dc, timeout, RpcCallback.fromUnwrapFuture(r));
        return r;
    }

    public CompletableFuture<Long> commitChange(int groupId, long prepareIndex, DtTime timeout) {
        AdminCommitOrAbortReq req = new AdminCommitOrAbortReq();
        req.groupId = groupId;
        req.prepareIndex = prepareIndex;
        SimpleWritePacket p = new SimpleWritePacket(Commands.RAFT_ADMIN_COMMIT_CHANGE, req);

        DecoderCallbackCreator<Long> dc = PbLongCallback.CALLBACK_CREATOR;
        CompletableFuture<Long> r = new CompletableFuture<>();
        sendRequest(groupId, p, dc, timeout, RpcCallback.fromUnwrapFuture(r));
        return r;
    }

    public CompletableFuture<Long> abortChange(int groupId, DtTime timeout) {
        AdminCommitOrAbortReq req = new AdminCommitOrAbortReq();
        req.groupId = groupId;
        SimpleWritePacket p = new SimpleWritePacket(Commands.RAFT_ADMIN_ABORT_CHANGE, req);

        DecoderCallbackCreator<Long> dc = PbLongCallback.CALLBACK_CREATOR;
        CompletableFuture<Long> r = new CompletableFuture<>();
        sendRequest(groupId, p, dc, timeout, RpcCallback.fromUnwrapFuture(r));
        return r;
    }

    private CompletableFuture<Void> sendByNodeId(int nodeId, DtTime timeout, WritePacket p) {
        return sendByNodeId(nodeId, timeout, p, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR);
    }

    private <T> CompletableFuture<T> sendByNodeId(int nodeId, DtTime timeout, WritePacket p, DecoderCallbackCreator<T> dc) {
        RaftNode n = getNode(nodeId);
        if (n == null) {
            return DtUtil.failedFuture(new RaftException("node not found: " + nodeId));
        }
        CompletableFuture<T> f = new CompletableFuture<>();
        nioClient.sendRequest(n.peer, p, dc, timeout, RpcCallback.fromUnwrapFuture(f));
        return f;
    }

    public CompletableFuture<Void> serverAddGroup(int nodeId, int groupId, String members, String observers, DtTime timeout) {
        AdminAddGroupReq req = new AdminAddGroupReq();
        req.groupId = groupId;
        req.nodeIdOfMembers = members;
        req.nodeIdOfObservers = observers;
        SimpleWritePacket p = new SimpleWritePacket(Commands.RAFT_ADMIN_ADD_GROUP, req);
        return sendByNodeId(nodeId, timeout, p);
    }

    public CompletableFuture<Void> serverRemoveGroup(int nodeId, int groupId, DtTime timeout) {
        PbIntWritePacket p = new PbIntWritePacket(Commands.RAFT_ADMIN_REMOVE_GROUP, groupId);
        return sendByNodeId(nodeId, timeout, p);
    }

    public CompletableFuture<Void> serverAddNode(int nodeIdToInvoke, int nodeIdToAdd, String host, int port) {
        AdminAddNodeReq req = new AdminAddNodeReq();
        req.nodeId = nodeIdToAdd;
        req.host = host;
        req.port = port;
        SimpleWritePacket p = new SimpleWritePacket(Commands.RAFT_ADMIN_ADD_NODE, req);
        return sendByNodeId(nodeIdToInvoke, createDefaultTimeout(), p);
    }

    public CompletableFuture<Void> serverRemoveNode(int nodeIdToInvoke, int nodeIdToRemove) {
        PbIntWritePacket p = new PbIntWritePacket(Commands.RAFT_ADMIN_REMOVE_NODE, nodeIdToRemove);
        return sendByNodeId(nodeIdToInvoke, createDefaultTimeout(), p);
    }

    public CompletableFuture<List<RaftNode>> serverListNodes(int nodeId) {
        EmptyBodyReqPacket p = new EmptyBodyReqPacket(Commands.RAFT_ADMIN_LIST_NODES);
        return sendByNodeId(nodeId, createDefaultTimeout(), p, ctx -> ctx.toDecoderCallback(new AdminListNodesResp()))
                .thenApply(resp -> {
                    Collections.sort(resp.nodes);
                    return resp.nodes;
                });
    }

    public CompletableFuture<int[]> serverListGroups(int groupId) {
        EmptyBodyReqPacket p = new EmptyBodyReqPacket(Commands.RAFT_ADMIN_LIST_GROUPS);
        return sendByNodeId(groupId, createDefaultTimeout(), p, ctx -> ctx.toDecoderCallback(new AdminListGroupsResp()))
                .thenApply(resp -> {
                    Arrays.sort(resp.groupIds);
                    return resp.groupIds;
                });
    }
}