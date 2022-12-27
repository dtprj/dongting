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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.Decoder;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.ProcessContext;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.raft.rpc.AppendReqWriteFrame;
import com.github.dtprj.dongting.raft.rpc.AppendRespCallback;
import com.github.dtprj.dongting.raft.rpc.VoteReq;
import com.github.dtprj.dongting.raft.rpc.VoteResp;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class RaftRpc {

    private static final DtLog log = DtLogs.getLogger(RaftRpc.class);

    private final NioClient client;
    private final RaftServerConfig config;
    private final RaftStatus raftStatus;
    private final LinkedBlockingQueue<RaftTask> queue;

    public RaftRpc(NioClient client, RaftServerConfig config, RaftStatus raftStatus, LinkedBlockingQueue<RaftTask> queue) {
        this.client = client;
        this.config = config;
        this.raftStatus = raftStatus;
        this.queue = queue;
    }

    public void sendVoteRequest(RaftNode node, Supplier<Boolean> electTimeout) {
        VoteReq req = new VoteReq();
        req.setCandidateId(config.getId());
        req.setTerm(raftStatus.getCurrentTerm());
        // TODO log fields
        VoteReq.WriteFrame wf = new VoteReq.WriteFrame(req);
        wf.setCommand(Commands.RAFT_REQUEST_VOTE);
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        Decoder decoder = new PbZeroCopyDecoder() {
            @Override
            protected PbCallback createCallback(ProcessContext context) {
                return new VoteResp.Callback();
            }
        };
        CompletableFuture<ReadFrame> f = client.sendRequest(node.getPeer(), wf, decoder, timeout);
        log.info("send vote request to {}, term={}, votes={}", node.getPeer().getEndPoint(),
                raftStatus.getCurrentTerm(), raftStatus.getCurrentVotes());
        f.handle((rf, ex) -> {
            if (ex == null) {
                RaftTask t = new RaftTask();
                t.setType(RaftTask.TYPE_REQUEST_VOTE_RESP);
                t.setData(rf);
                t.setRemoteNode(node);
                queue.offer(t);
            } else {
                log.warn("request vote rpc fail. remote={}, error={}",
                        node.getPeer().getEndPoint(), ex.toString());
                if (!electTimeout.get()) {
                    sendVoteRequest(node, electTimeout);
                }
            }
            return null;
        });
    }

    public void sendHeartBeat(RaftNode node) {
        AppendReqWriteFrame req = new AppendReqWriteFrame();
        req.setTerm(raftStatus.getCurrentTerm());
        req.setLeaderId(config.getId());
        req.setCommand(Commands.RAFT_APPEND_ENTRIES);
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        Decoder decoder = new PbZeroCopyDecoder() {
            @Override
            protected PbCallback createCallback(ProcessContext context) {
                return new AppendRespCallback();
            }
        };
        CompletableFuture<ReadFrame> f = client.sendRequest(node.getPeer(), req, decoder, timeout);
        f.handle((rf, ex) -> {
            if (ex == null) {
                RaftTask t = new RaftTask();
                t.setType(RaftTask.TYPE_APPEND_ENTRIES_RESP);
                t.setData(rf);
                t.setRemoteNode(node);
                queue.offer(t);
            } else {
                // TODO
            }
            return null;
        });
    }
}
