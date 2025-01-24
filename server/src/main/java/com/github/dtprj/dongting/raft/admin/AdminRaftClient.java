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
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.raft.GroupInfo;
import com.github.dtprj.dongting.raft.RaftClient;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.rpc.TransferLeaderReq;

import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class AdminRaftClient extends RaftClient {

    public AdminRaftClient() {
        this(new NioClientConfig());
    }

    public AdminRaftClient(NioClientConfig nioClientConfig) {
        super(nioClientConfig);
    }

    public CompletableFuture<Void> transferLeader(int groupId, int oldLeader, int newLeader, DtTime timeout) {
        if (oldLeader == newLeader) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "old and new leader id equals: " + oldLeader));
        }
        return updateLeaderInfo(groupId).thenCompose(group -> transferLeader(group, oldLeader, newLeader, timeout));
    }

    private CompletableFuture<Void> transferLeader(GroupInfo group, int oldLeader, int newLeader, DtTime timeout) {
        if (group.getLeader().getNodeId() != oldLeader) {
            throw new RaftException("old leader not match");
        }
        boolean foundNewLeader = false;
        for (RaftNode n : group.getServers()) {
            if (n.getNodeId() == newLeader) {
                foundNewLeader = true;
                break;
            }
        }
        if (!foundNewLeader) {
            throw new RaftException("new leader not found is servers list: " + newLeader);
        }
        TransferLeaderReq req = new TransferLeaderReq();
        req.groupId = group.getGroupId();
        req.oldLeaderId = oldLeader;
        req.newLeaderId = newLeader;
        SimpleWritePacket p = new SimpleWritePacket(req);
        p.setCommand(Commands.RAFT_ADMIN_TRANSFER_LEADER);
        DecoderCallbackCreator<Void> dc = DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR;
        CompletableFuture<ReadPacket<Void>> f = new CompletableFuture<>();
        nioClient.sendRequest(group.getLeader().getPeer(), p, dc, timeout, RpcCallback.fromFuture(f));
        return f.thenApply(rp -> null);
    }
}