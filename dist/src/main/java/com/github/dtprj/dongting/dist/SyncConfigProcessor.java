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
package com.github.dtprj.dongting.dist;

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.impl.MembersInfo;
import com.github.dtprj.dongting.raft.impl.NodeManager;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftServer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public class SyncConfigProcessor extends ReqProcessor<Void> {

    private final RaftServer server;
    private final File serversFile;
    private final ReentrantLock lock = new ReentrantLock();

    public SyncConfigProcessor(RaftServer server, File serversFile) {
        this.server = server;
        this.serversFile = serversFile;
    }

    @Override
    public DecoderCallback<Void> createDecoderCallback(int command, DecodeContext context) {
        return DecoderCallback.VOID_DECODE_CALLBACK;
    }

    @Override
    public WritePacket process(ReadPacket<Void> packet, ReqContext reqContext) throws Exception {
        boolean servicePort = RaftProcessor.requestServicePort(reqContext, server.getServerConfig());
        if (!RaftProcessor.checkPort(servicePort, false, true)) {
            return RaftProcessor.createWrongPortRest(packet, reqContext);
        }
        List<RaftNode> allNodes;
        List<MembersInfo> groupsInfos;
        NodeManager nm = server.getNodeManager();
        nm.getLock().lock();
        try {
            allNodes = nm.getAllNodes();
            groupsInfos = server.getRaftGroups().values().stream()
                    .map(rg -> rg.groupComponents.raftStatus.membersInfo)
                    .toList();
        } finally {
            nm.getLock().unlock();
        }
        boolean locked = lock.tryLock(5, TimeUnit.SECONDS);
        if (locked) {
            try {
                syncConfig(allNodes, groupsInfos);
                return new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            } finally {
                lock.unlock();
            }
        } else {
            EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
            p.msg = "sync lock timeout";
            return p;
        }
    }

    private void syncConfig(List<RaftNode> nodes, List<MembersInfo> groupInfos) throws IOException {
        // TODO
    }
}
