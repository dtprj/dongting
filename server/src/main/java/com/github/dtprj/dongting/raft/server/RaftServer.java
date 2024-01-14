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

import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.buf.TwoLevelPool;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.fiber.Dispatcher;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.NioConfig;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.NioServerConfig;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.ApplyManager;
import com.github.dtprj.dongting.raft.impl.CommitManager;
import com.github.dtprj.dongting.raft.impl.EventBus;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.LinearTaskRunner;
import com.github.dtprj.dongting.raft.impl.MemberManager;
import com.github.dtprj.dongting.raft.impl.NodeManager;
import com.github.dtprj.dongting.raft.impl.PendingStat;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftGroups;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.ReplicateManager;
import com.github.dtprj.dongting.raft.impl.VoteManager;
import com.github.dtprj.dongting.raft.rpc.NodePingProcessor;
import com.github.dtprj.dongting.raft.rpc.RaftGroupProcessor;
import com.github.dtprj.dongting.raft.rpc.RaftPingProcessor;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author huangli
 */
public class RaftServer extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftServer.class);
    private final RaftFactory raftFactory;

    private final NioServer replicateNioServer;
    private final NioClient replicateNioClient;
    private final NioServer serviceNioServer;

    private final RaftGroups raftGroups = new RaftGroups();

    private final RaftServerConfig serverConfig;

    private final NodeManager nodeManager;

    private final AtomicBoolean change = new AtomicBoolean(false);

    private final PendingStat serverStat = new PendingStat();

    private final CompletableFuture<Void> readyFuture = new CompletableFuture<>();

    private final List<RaftGroupProcessor<?>> raftGroupProcessors = new ArrayList<>();

    public RaftServer(RaftServerConfig serverConfig, List<RaftGroupConfig> groupConfig, RaftFactory raftFactory) {
        Objects.requireNonNull(serverConfig);
        Objects.requireNonNull(groupConfig);
        Objects.requireNonNull(raftFactory);
        this.serverConfig = serverConfig;
        this.raftFactory = raftFactory;

        Objects.requireNonNull(serverConfig.getServers());
        DtUtil.checkPositive(serverConfig.getNodeId(), "id");
        DtUtil.checkPositive(serverConfig.getReplicatePort(), "replicatePort");

        List<RaftNode> allRaftServers = RaftUtil.parseServers(serverConfig.getNodeId(), serverConfig.getServers());
        HashSet<Integer> allNodeIds = new HashSet<>();
        HashSet<HostPort> allNodeHosts = new HashSet<>();
        for (RaftNode rn : allRaftServers) {
            if (!allNodeIds.add(rn.getNodeId())) {
                throw new IllegalArgumentException("duplicate server id: " + rn.getNodeId());
            }
            if (!allNodeHosts.add(rn.getHostPort())) {
                throw new IllegalArgumentException("duplicate server host: " + rn.getHostPort());
            }
        }
        if (!allNodeIds.contains(serverConfig.getNodeId())) {
            throw new IllegalArgumentException("self id not found in servers list: " + serverConfig.getNodeId());
        }

        NioClientConfig repClientConfig = new NioClientConfig();
        repClientConfig.setName("RaftClient");
        setupNioConfig(repClientConfig);
        replicateNioClient = new NioClient(repClientConfig);

        createRaftGroups(serverConfig, groupConfig, allNodeIds);
        nodeManager = new NodeManager(serverConfig, allRaftServers, replicateNioClient, raftGroups);
        raftGroups.forEach((id, g) -> g.getGroupComponents().getEventBus().register(nodeManager));

        NioServerConfig repServerConfig = new NioServerConfig();
        repServerConfig.setPort(serverConfig.getReplicatePort());
        repServerConfig.setName("RaftRepServer");
        repServerConfig.setBizThreads(0);
        // use multi io threads
        setupNioConfig(repServerConfig);
        replicateNioServer = new NioServer(repServerConfig);

        replicateNioServer.register(Commands.NODE_PING, new NodePingProcessor(serverConfig.getNodeId(), nodeManager.getUuid()));
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_PING, new RaftPingProcessor(this));
        /* TODO
        replicateNioServer.register(Commands.RAFT_APPEND_ENTRIES, new AppendProcessor(this, raftGroups));
        replicateNioServer.register(Commands.RAFT_REQUEST_VOTE, new VoteProcessor(this));
        replicateNioServer.register(Commands.RAFT_INSTALL_SNAPSHOT, new InstallSnapshotProcessor(this));
        replicateNioServer.register(Commands.RAFT_LEADER_TRANSFER, new TransferLeaderProcessor(this));
        replicateNioServer.register(Commands.RAFT_QUERY_STATUS, new QueryStatusProcessor(this));
        */

        if (serverConfig.getServicePort() > 0) {
            NioServerConfig serviceServerConfig = new NioServerConfig();
            serviceServerConfig.setPort(serverConfig.getServicePort());
            serviceServerConfig.setName("RaftServiceServer");
            serviceServerConfig.setBizThreads(0);
            // use multi io threads
            serviceNioServer = new NioServer(serviceServerConfig);
            // TODO
            // serviceNioServer.register(Commands.RAFT_QUERY_LEADER, new QueryLeaderProcessor(this));
        } else {
            serviceNioServer = null;
        }
    }

    private void addRaftGroupProcessor(NioServer nioServer, int command, RaftGroupProcessor<?> processor) {
        nioServer.register(command, processor);
        raftGroupProcessors.add(processor);
    }

    private void setupNioConfig(NioConfig nc) {
        nc.setFinishPendingImmediatelyWhenChannelClose(true);
        nc.setMaxOutRequests(0);
        nc.setMaxInRequests(0);
        nc.setMaxInBytes(0);
        nc.setMaxBodySize(Integer.MAX_VALUE);
        nc.setMaxFrameSize(Integer.MAX_VALUE);
    }

    private void createRaftGroups(RaftServerConfig serverConfig,
                                  List<RaftGroupConfig> groupConfig, HashSet<Integer> allNodeIds) {
        for (RaftGroupConfig rgc : groupConfig) {
            RaftGroupImpl gc = createRaftGroup(serverConfig, allNodeIds, rgc);
            if (raftGroups.get(rgc.getGroupId()) != null) {
                throw new IllegalArgumentException("duplicate group id: " + rgc.getGroupId());
            }
            raftGroups.put(rgc.getGroupId(), gc);
        }

    }

    private RaftGroupImpl createRaftGroup(RaftServerConfig serverConfig,
                                          Set<Integer> allNodeIds, RaftGroupConfig rgc) {
        Objects.requireNonNull(rgc.getNodeIdOfMembers());

        EventBus eventBus = new EventBus();

        HashSet<Integer> nodeIdOfMembers = new HashSet<>();
        parseMemberIds(allNodeIds, nodeIdOfMembers, rgc.getNodeIdOfMembers(), rgc.getGroupId());
        if (nodeIdOfMembers.isEmpty() && serverConfig.isStaticConfig()) {
            throw new IllegalArgumentException("no member in group: " + rgc.getGroupId());
        }

        Set<Integer> nodeIdOfObservers;
        if (rgc.getNodeIdOfObservers() != null && !rgc.getNodeIdOfObservers().trim().isEmpty()) {
            nodeIdOfObservers = new HashSet<>();
            parseMemberIds(allNodeIds, nodeIdOfObservers, rgc.getNodeIdOfObservers(), rgc.getGroupId());
            for (int id : nodeIdOfMembers) {
                if (nodeIdOfObservers.contains(id)) {
                    throw new IllegalArgumentException("member and observer has same node: " + id);
                }
            }
        } else {
            nodeIdOfObservers = Collections.emptySet();
        }

        boolean isMember = nodeIdOfMembers.contains(serverConfig.getNodeId());
        boolean isObserver = nodeIdOfObservers.contains(serverConfig.getNodeId());
        if (!isMember && !isObserver && serverConfig.isStaticConfig()) {
            throw new IllegalArgumentException("self id not found in group members/observers list: " + serverConfig.getNodeId());
        }

        FiberGroup fiberGroup = raftFactory.createFiberGroup(rgc);
        RaftStatusImpl raftStatus = new RaftStatusImpl(fiberGroup.getDispatcher().getTs());
        raftStatus.setNodeIdOfMembers(nodeIdOfMembers);
        raftStatus.setNodeIdOfObservers(nodeIdOfObservers);
        raftStatus.setGroupId(rgc.getGroupId());

        RaftGroupConfigEx rgcEx = createGroupConfigEx(rgc, raftStatus, fiberGroup);


        StateMachine stateMachine = raftFactory.createStateMachine(rgcEx);
        rgcEx.setCodecFactory(stateMachine);
        StatusManager statusManager = new StatusManager(rgcEx);
        RaftLog raftLog = raftFactory.createRaftLog(rgcEx, statusManager);

        MemberManager memberManager = new MemberManager(serverConfig, rgcEx, replicateNioClient,
                raftStatus, eventBus);
        ApplyManager applyManager = new ApplyManager(serverConfig.getNodeId(), raftLog, stateMachine, raftStatus,
                eventBus, rgcEx.getHeapPool(), statusManager);
        CommitManager commitManager = new CommitManager(raftStatus, applyManager);
        ReplicateManager replicateManager = new ReplicateManager(serverConfig, rgcEx, raftStatus, raftLog,
                stateMachine, replicateNioClient, commitManager, statusManager);

        LinearTaskRunner linearTaskRunner = new LinearTaskRunner(rgcEx, raftStatus, applyManager);
        VoteManager voteManager = new VoteManager(serverConfig, rgc.getGroupId(), raftStatus, replicateNioClient,
                linearTaskRunner, statusManager);

        eventBus.register(linearTaskRunner);
        eventBus.register(voteManager);

        GroupComponents gc = new GroupComponents();
        gc.setServerConfig(serverConfig);
        gc.setGroupConfig(rgcEx);
        gc.setRaftLog(raftLog);
        gc.setStateMachine(stateMachine);
        gc.setRaftStatus(raftStatus);
        gc.setMemberManager(memberManager);
        gc.setVoteManager(voteManager);
        gc.setCommitManager(commitManager);
        gc.setApplyManager(applyManager);
        gc.setEventBus(eventBus);
        gc.setNodeManager(nodeManager);
        gc.setServerStat(serverStat);
        gc.setSnapshotManager(raftFactory.createSnapshotManager(rgcEx));
        gc.setStatusManager(statusManager);
        gc.setFiberGroup(fiberGroup);
        return new RaftGroupImpl(gc);
    }

    private RaftGroupConfigEx createGroupConfigEx(RaftGroupConfig rgc, RaftStatusImpl raftStatus,
                                                  FiberGroup fiberGroup) {
        RaftGroupConfigEx rgcEx = new RaftGroupConfigEx(rgc.getGroupId(), rgc.getNodeIdOfMembers(),
                rgc.getNodeIdOfObservers());
        rgcEx.setDataDir(rgc.getDataDir());
        rgcEx.setStatusFile(rgc.getStatusFile());
        rgcEx.setIoRetryInterval(rgc.getIoRetryInterval());
        rgcEx.setIoTimeout(rgc.getIoTimeout());
        rgcEx.setAllocateTimeout(rgc.getAllocateTimeout());

        rgcEx.setTs(raftStatus.getTs());
        rgcEx.setHeapPool(createHeapPoolFactory(fiberGroup));
        rgcEx.setDirectPool(serverConfig.getPoolFactory().apply(raftStatus.getTs(), true));
        rgcEx.setRaftStatus(raftStatus);
        rgcEx.setIoExecutor(raftFactory.createIoExecutor());
        rgcEx.setFiberGroup(fiberGroup);

        return rgcEx;
    }

    private RefBufferFactory createHeapPoolFactory(FiberGroup fiberGroup) {
        Dispatcher dispatcher = fiberGroup.getDispatcher();
        ExecutorService executorService = dispatcher.getExecutor();

        TwoLevelPool heapPool = (TwoLevelPool) serverConfig.getPoolFactory().apply(dispatcher.getTs(), false);
        TwoLevelPool releaseSafePool = heapPool.toReleaseInOtherThreadInstance(dispatcher.getThread(), byteBuffer -> {
            if (byteBuffer != null) {
                executorService.execute(() -> heapPool.release(byteBuffer));
            }
        });
        return new RefBufferFactory(releaseSafePool, 800);
    }

    private static void parseMemberIds(Set<Integer> allNodeIds, Set<Integer> nodeIdOfMembers, String str, int groupId) {
        String[] membersStr = str.split(",");
        for (String idStr : membersStr) {
            int id = Integer.parseInt(idStr.trim());
            if (!allNodeIds.contains(id)) {
                throw new IllegalArgumentException("member id " + id + " not in server list: groupId=" + groupId);
            }
            if (!nodeIdOfMembers.add(id)) {
                throw new IllegalArgumentException("duplicated raft member id " + id + ".  groupId=" + groupId);
            }
        }
    }

    @Override
    protected void doStart() {
        try {
            // start all fiber group
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
            raftGroups.forEach((groupId, g) -> {
                GroupComponents gc = g.getGroupComponents();
                gc.getMemberManager().init(nodeManager.getAllNodesEx());
                CompletableFuture<Void> f = gc.getFiberGroup().getDispatcher().startGroup(gc.getFiberGroup());
                futures.add(f);
            });
            // should complete soon, so we wait here
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);

            // init raft log and state machine
            futures.clear();
            raftGroups.forEach((groupId, g) -> {
                GroupComponents gc = g.getGroupComponents();
                InitFiberFrame initFiberFrame = new InitFiberFrame(gc, raftGroupProcessors);
                Fiber initFiber = new Fiber("init-raft-group-" + groupId,
                        gc.getFiberGroup(), initFiberFrame);
                gc.getFiberGroup().fireFiber(initFiber);
                futures.add(initFiberFrame.getPrepareFuture());
            });
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((v, ex) -> {
                if (ex != null) {
                    readyFuture.completeExceptionally(ex);
                } else if (status == STATUS_STARTING) {
                    startServers();
                } else {
                    readyFuture.completeExceptionally(new IllegalStateException("server is not starting"));
                }
            });
        } catch (Exception e) {
            log.error("start raft server failed", e);
            throw new RaftException(e);
        }
    }

    private void startServers() {
        try {
            // start replicate server and client
            replicateNioServer.start();
            replicateNioClient.start();
            replicateNioClient.waitStart();

            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();

            nodeManager.start();
            int electQuorum = RaftUtil.getElectQuorum(nodeManager.getAllNodesEx().size());
            futures.add(nodeManager.readyFuture(electQuorum));

            raftGroups.forEach((groupId, g) -> {
                GroupComponents gc = g.getGroupComponents();
                gc.getFiberGroup().fireFiber(gc.getMemberManager().createRaftPingFiber());
                CompletableFuture<Void> f = gc.getMemberManager().getStartReadyFuture();
                futures.add(f);
            });

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((v, ex) -> {
                if (ex != null) {
                    readyFuture.completeExceptionally(ex);
                }  else if (status == STATUS_STARTING) {
                    try {
                        serviceNioServer.start();
                        readyFuture.complete(null);
                    } catch (Exception serviceNioServerStartEx) {
                        readyFuture.completeExceptionally(serviceNioServerStartEx);
                    }
                } else {
                    readyFuture.completeExceptionally(new IllegalStateException("server is not starting"));
                }
            });
        } catch (Exception e) {
            log.error("start raft server failed", e);
            throw new RaftException(e);
        }
    }

    @SuppressWarnings("unused")
    public CompletableFuture<Void> getReadyFuture() {
        return readyFuture;
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
        try {
            if (serviceNioServer != null) {
                serviceNioServer.stop(timeout);
            }
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
            raftGroups.forEach((groupId, g) -> {
                g.getFiberGroup().requestShutdown();
                futures.add(g.getFiberGroup().getShutdownFuture());
            });
            nodeManager.stop(timeout);

            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(timeout.rest(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                DtUtil.restoreInterruptStatus();
            } catch (Exception e) {
                throw new RaftException(e);
            } finally {
                if (replicateNioServer != null) {
                    replicateNioServer.stop(timeout);
                }
                if (replicateNioClient != null) {
                    replicateNioClient.stop(timeout);
                }
            }
        } catch (RuntimeException | Error e) {
            log.error("stop raft server failed", e);
            throw e;
        }
    }

    public RaftGroup getRaftGroup(int groupId) {
        return raftGroups.get(groupId);
    }

    public NioServer getServiceNioServer() {
        return serviceNioServer;
    }
}
