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
import com.github.dtprj.dongting.common.CloseUtil;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.common.ObjUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.NioConfig;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.NioServerConfig;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.impl.GroupConManager;
import com.github.dtprj.dongting.raft.impl.Raft;
import com.github.dtprj.dongting.raft.impl.RaftComponents;
import com.github.dtprj.dongting.raft.impl.RaftExecutor;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatus;
import com.github.dtprj.dongting.raft.impl.RaftThread;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.ShareStatus;
import com.github.dtprj.dongting.raft.impl.StatusUtil;
import com.github.dtprj.dongting.raft.impl.VoteManager;
import com.github.dtprj.dongting.raft.rpc.AppendProcessor;
import com.github.dtprj.dongting.raft.rpc.InstallSnapshotProcessor;
import com.github.dtprj.dongting.raft.rpc.VoteProcessor;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author huangli
 */
public class RaftServer extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftServer.class);
    private final NioServer raftServer;
    private final NioClient raftClient;
    private final RaftThread raftThread;
    private final RaftStatus raftStatus;
    private final RaftLog raftLog;
    private final StateMachine stateMachine;

    private final RaftServerConfig config;

    private final int maxPendingWrites;
    private final long maxPendingWriteBytes;
    @SuppressWarnings({"unused"})
    private volatile int pendingWrites;
    @SuppressWarnings({"unused"})
    private volatile long pendingWriteBytes;

    private static final VarHandle PENDING_WRITES;
    private static final VarHandle PENDING_WRITE_BYTES;

    private final Timestamp readTimestamp = new Timestamp();

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            PENDING_WRITES = lookup.findVarHandle(RaftServer.class, "pendingWrites", int.class);
            PENDING_WRITE_BYTES = lookup.findVarHandle(RaftServer.class, "pendingWriteBytes", long.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public RaftServer(RaftServerConfig config, RaftLog raftLog, StateMachine stateMachine) {
        Objects.requireNonNull(config.getServers());
        ObjUtil.checkPositive(config.getId(), "id");
        ObjUtil.checkPositive(config.getRaftPort(), "port");
        this.raftLog = raftLog;
        this.config = config;
        this.stateMachine = stateMachine;
        this.maxPendingWrites = config.getMaxPendingWrites();
        this.maxPendingWriteBytes = config.getMaxPendingWriteBytes();


        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        RaftExecutor raftExecutor = new RaftExecutor(queue);

        IntObjMap<HostPort> raftServers = RaftUtil.parseServers(config.getServers());

        int electQuorum = raftServers.size() / 2 + 1;
        int rwQuorum = raftServers.size() >= 4 && raftServers.size() % 2 == 0 ? raftServers.size() / 2 : electQuorum;
        raftStatus = new RaftStatus(electQuorum, rwQuorum);
        raftStatus.setRaftExecutor(raftExecutor);

        NioClientConfig nioClientConfig = new NioClientConfig();
        nioClientConfig.setName("RaftClient");
        setupNioConfig(nioClientConfig);
        raftClient = new NioClient(nioClientConfig);



        RaftComponents container = new RaftComponents();
        container.setRaftExecutor(raftExecutor);
        container.setRaftLog(raftLog);
        container.setRaftStatus(raftStatus);
        container.setClient(raftClient);
        container.setConfig(config);
        container.setStateMachine(stateMachine);

        GroupConManager groupConManager = new GroupConManager(config, raftClient, raftExecutor, raftStatus);
        Raft raft = new Raft(container);
        VoteManager voteManager = new VoteManager(container, raft);
        raftThread = new RaftThread(container, raft, groupConManager, voteManager);

        NioServerConfig nioServerConfig = new NioServerConfig();
        nioServerConfig.setPort(config.getRaftPort());
        nioServerConfig.setName("RaftServer");
        nioServerConfig.setBizThreads(0);
        nioServerConfig.setIoThreads(1);
        setupNioConfig(nioServerConfig);
        raftServer = new NioServer(nioServerConfig);
        raftServer.register(Commands.RAFT_PING, groupConManager.getProcessor(), raftExecutor);
        AppendProcessor ap = new AppendProcessor(raftStatus, raftLog, stateMachine, voteManager);
        raftServer.register(Commands.RAFT_APPEND_ENTRIES, ap, raftExecutor);
        raftServer.register(Commands.RAFT_REQUEST_VOTE, new VoteProcessor(raftStatus), raftExecutor);
        raftServer.register(Commands.RAFT_INSTALL_SNAPSHOT, new InstallSnapshotProcessor(raftStatus, stateMachine), raftExecutor);
    }

    private void setupNioConfig(NioConfig nc) {
        nc.setMaxOutRequests(0);
        nc.setMaxInRequests(0);
        nc.setMaxInBytes(0);
        nc.setMaxBodySize(Integer.MAX_VALUE);
        nc.setMaxFrameSize(Integer.MAX_VALUE);
    }

    @Override
    protected void doStart() {
        StatusUtil.initStatusFileChannel(config.getDataDir(), config.getStatusFile(), raftStatus);

        Pair<Integer, Long> initResult = raftLog.init();
        stateMachine.init(raftLog);
        raftStatus.setLastLogTerm(initResult.getLeft());
        raftStatus.setLastLogIndex(initResult.getRight());
        raftServer.start();
        raftClient.start();
        raftClient.waitStart();
        raftThread.start();
        raftThread.waitInit();
    }

    @Override
    protected void doStop() {
        raftServer.stop();
        raftClient.stop();
        raftThread.requestShutdown();
        raftThread.interrupt();
        try {
            raftThread.join(100);
        } catch (InterruptedException e) {
            throw new RaftException(e);
        } finally {
            CloseUtil.close(raftStatus.getStatusFileLock(), raftStatus.getStatusFile());
        }
    }

    public CompletableFuture<RaftOutput> submitLinearTask(RaftInput input) throws RaftException {
        if (raftStatus.isError()) {
            throw new RaftException("raft status is error");
        }
        int size = input.size();
        if (size > config.getMaxBodySize()) {
            throw new RaftException("request size too large, size=" + size + ", maxBodySize=" + config.getMaxBodySize());
        }
        int currentPendingWrites = (int) PENDING_WRITES.getAndAddRelease(this, 1);
        if (currentPendingWrites >= maxPendingWrites) {
            String msg = "submitRaftTask failed: too many pending writes, currentPendingWrites=" + currentPendingWrites;
            log.warn(msg);
            PENDING_WRITES.getAndAddRelease(this, -1);
            throw new RaftException(msg);
        }
        long currentPendingWriteBytes = (long) PENDING_WRITE_BYTES.getAndAddRelease(this, size);
        if (currentPendingWriteBytes >= maxPendingWriteBytes) {
            String msg = "submitRaftTask failed: too many pending write bytes,currentPendingWriteBytes="
                    + currentPendingWriteBytes + ", currentRequestBytes=" + size;
            log.warn(msg);
            PENDING_WRITE_BYTES.getAndAddRelease(this, -size);
            throw new RaftException(msg);
        }
        CompletableFuture<RaftOutput> f = raftThread.submitRaftTask(input);
        registerCallback(f, size);
        return f;
    }

    private void registerCallback(CompletableFuture<?> f, int size) {
        f.whenComplete((o, ex) -> {
            PENDING_WRITES.getAndAddRelease(this, -1);
            PENDING_WRITE_BYTES.getAndAddRelease(this, -size);
        });
    }

    public long getLogIndexForRead(DtTime deadline)
            throws RaftException, InterruptedException, TimeoutException {
        if (raftStatus.isError()) {
            throw new RaftException("raft status error");
        }
        ShareStatus ss = raftStatus.getShareStatus();
        readTimestamp.refresh(1);
        if (ss.role != RaftRole.leader) {
            throw new NotLeaderException(RaftUtil.getLeader(ss.currentLeader));
        }
        long t = readTimestamp.getNanoTime();
        if (ss.leaseEndNanos - t < 0) {
            throw new NotLeaderException(null);
        }
        if (ss.firstCommitOfApplied != null) {
            try {
                ss.firstCommitOfApplied.get(deadline.rest(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
            } catch (ExecutionException e) {
                BugLog.log(e);
                throw new RaftException(e);
            }
        }
        return ss.lastApplied;
    }

}
