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

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.ObjUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.NioServerConfig;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.impl.GroupConManager;
import com.github.dtprj.dongting.raft.impl.MemKv;
import com.github.dtprj.dongting.raft.impl.MemRaftLog;
import com.github.dtprj.dongting.raft.impl.RaftExecutor;
import com.github.dtprj.dongting.raft.impl.RaftLog;
import com.github.dtprj.dongting.raft.impl.RaftRpc;
import com.github.dtprj.dongting.raft.impl.RaftStatus;
import com.github.dtprj.dongting.raft.impl.RaftThread;
import com.github.dtprj.dongting.raft.impl.StateMachine;
import com.github.dtprj.dongting.raft.rpc.AppendProcessor;
import com.github.dtprj.dongting.raft.rpc.VoteProcessor;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author huangli
 */
public class RaftServer extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftServer.class);
    private final RaftServerConfig config;
    private final NioServer server;
    private final NioClient client;
    private final Set<HostPort> servers;
    private final GroupConManager groupConManager;
    private final RaftThread raftThread;
    private final RaftRpc raftRpc;
    private final RaftStatus raftStatus;
    private final RaftLog raftLog;
    private final StateMachine stateMachine;

    public RaftServer(RaftServerConfig config) {
        this.config = config;
        Objects.requireNonNull(config.getServers());
        ObjUtil.checkPositive(config.getId(), "id");
        ObjUtil.checkPositive(config.getPort(), "port");

        servers = GroupConManager.parseServers(config.getServers());

        int electQuorum = servers.size() / 2 + 1;
        int rwQuorum = servers.size() % 2 == 0 ? servers.size() / 2 : electQuorum;
        raftStatus = new RaftStatus(electQuorum, rwQuorum);

        raftLog = new MemRaftLog();
        stateMachine = new MemKv();

        NioServerConfig nioServerConfig = new NioServerConfig();
        nioServerConfig.setPort(config.getPort());
        nioServerConfig.setName("RaftServer");
        nioServerConfig.setBizThreads(0);
        nioServerConfig.setIoThreads(1);
        server = new NioServer(nioServerConfig);

        NioClientConfig nioClientConfig = new NioClientConfig();
        nioClientConfig.setName("RaftClient");
        client = new NioClient(nioClientConfig);

        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        RaftExecutor executor = new RaftExecutor(queue);

        groupConManager = new GroupConManager(config, client, executor);
        server.register(Commands.RAFT_PING, this.groupConManager.getProcessor(), executor);
        server.register(Commands.RAFT_APPEND_ENTRIES, new AppendProcessor(raftStatus), executor);
        server.register(Commands.RAFT_REQUEST_VOTE, new VoteProcessor(raftStatus), executor);

        raftRpc = new RaftRpc(client, config, raftStatus, executor);
        raftThread = new RaftThread(config, executor, raftStatus, raftRpc, groupConManager);
    }

    @Override
    protected void doStart() {
        raftLog.load(stateMachine);
        server.start();
        client.start();
        client.waitStart();
        groupConManager.initRaftGroup(raftStatus.getElectQuorum(), servers, 1000);
        raftThread.start();
    }

    @Override
    protected void doStop() {
        server.stop();
        client.stop();
        raftThread.requestShutdown();
        raftThread.interrupt();
        try {
            raftThread.join(100);
        } catch (InterruptedException e) {
            throw new RaftException(e);
        }
    }

}
