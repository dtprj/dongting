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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.Decoder;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.pb.PbUtil;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class MemberManager {
    private static final DtLog log = DtLogs.getLogger(MemberManager.class);
    private final RaftServerConfig config;
    private final HashSet<Integer> ids = new HashSet<>();
    private final NioClient client;
    private final Executor executor;

    private RaftMember self;
    private List<RaftMember> otherMembers;

    private static final PbZeroCopyDecoder DECODER = new PbZeroCopyDecoder(context ->
            new RaftPingFrameCallback());

    private final ReqProcessor processor = new ReqProcessor() {
        @Override
        public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
            return new RaftPingWriteFrame();
        }

        @Override
        public Decoder getDecoder() {
            return DECODER;
        }
    };

    public MemberManager(RaftServerConfig config, NioClient client, Executor executor) {
        this.config = config;
        this.client = client;
        this.executor = executor;
    }

    public void init(RaftNodeEx selfNodeEx, List<RaftNodeEx> otherNodes) {
        for (RaftNodeEx node : otherNodes) {
            otherMembers.add(new RaftMember(node));
            ids.add(node.getId());
        }
        this.self = new RaftMember(selfNodeEx);
    }

    public void ensureRaftMemberStatus() {
        for (RaftMember member : otherMembers) {
            RaftNodeEx node = member.getNode();
            NodeStatus nodeStatus = node.getStatus();
            if (!nodeStatus.isReady()) {
                member.setReady(false);
            } else if (nodeStatus.getEpoch() != member.getEpoch()) {
                member.setReady(false);
                if (!member.isPinging()) {
                    raftPing(node, member);
                }
            }
        }
    }

    private void raftPing(RaftNodeEx raftNodeEx, RaftMember member) {
        if (raftNodeEx.getPeer().getStatus() != PeerStatus.connected) {
            member.setReady(false);
            return;
        }

        member.setPinging(true);
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        client.sendRequest(raftNodeEx.getPeer(), new RaftPingWriteFrame(), DECODER, timeout)
                .whenCompleteAsync((rf, ex) -> processPingResult(raftNodeEx, member, rf, ex), executor);
    }

    private void processPingResult(RaftNodeEx raftNodeEx, RaftMember member,
                                   ReadFrame rf, Throwable ex) {
        RaftPingFrameCallback callback = (RaftPingFrameCallback) rf.getBody();
        member.setPinging(false);
        if (ex != null) {
            log.warn("raft ping fail, remote={}", raftNodeEx.getHostPort(), ex);
            member.setReady(false);
        } else {
            if (ids.equals(callback.ids)) {
                log.info("raft ping success, id={}, servers={}, remote={}",
                        callback.id, callback.ids, raftNodeEx.getHostPort());
                member.setReady(true);
            } else {
                log.error("raft ping error, group ids not match: localIds={}, remoteIds={}, remote={}",
                        ids, callback.ids, raftNodeEx.getHostPort());
                member.setReady(false);
            }
        }
    }

    public ReqProcessor getProcessor() {
        return processor;
    }

    class RaftPingWriteFrame extends WriteFrame {

        public RaftPingWriteFrame() {
            setCommand(Commands.RAFT_PING);
        }

        @Override
        protected int calcEstimateBodySize() {
            int size = PbUtil.accurateFix32Size(1, config.getId());
            for (int id : ids) {
                size += PbUtil.accurateFix32Size(2, id);
            }
            return size;
        }

        @Override
        protected void encodeBody(ByteBuffer buf, ByteBufferPool pool) {
            super.writeBodySize(buf, estimateBodySize());
            PbUtil.writeFix32(buf, 1, config.getId());
            for (int id : ids) {
                PbUtil.writeFix32(buf, 2, id);
            }
        }
    }


    static class RaftPingFrameCallback extends PbCallback {
        private int id;
        private HashSet<Integer> ids = new HashSet<>();

        @Override
        public boolean readFix32(int index, int value) {
            if (index == 1) {
                this.id = value;
            } else if (index == 2) {
                ids.add(value);
            }
            return true;
        }

        @Override
        public Object getResult() {
            return this;
        }
    }

}
