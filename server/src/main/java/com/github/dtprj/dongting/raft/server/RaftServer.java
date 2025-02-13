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
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.fiber.Dispatcher;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
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
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.impl.ApplyManager;
import com.github.dtprj.dongting.raft.impl.CommitManager;
import com.github.dtprj.dongting.raft.impl.DecodeContextEx;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.InitFiberFrame;
import com.github.dtprj.dongting.raft.impl.LinearTaskRunner;
import com.github.dtprj.dongting.raft.impl.MemberManager;
import com.github.dtprj.dongting.raft.impl.NodeManager;
import com.github.dtprj.dongting.raft.impl.PendingStat;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.ReplicateManager;
import com.github.dtprj.dongting.raft.impl.ShareStatus;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.impl.VoteManager;
import com.github.dtprj.dongting.raft.rpc.AdminConfigChangeProcessor;
import com.github.dtprj.dongting.raft.rpc.AdminGroupAndNodeProcessor;
import com.github.dtprj.dongting.raft.rpc.AdminTransferLeaderProcessor;
import com.github.dtprj.dongting.raft.rpc.AppendProcessor;
import com.github.dtprj.dongting.raft.rpc.NodePingProcessor;
import com.github.dtprj.dongting.raft.rpc.QueryStatusProcessor;
import com.github.dtprj.dongting.raft.rpc.RaftPingProcessor;
import com.github.dtprj.dongting.raft.rpc.RaftSequenceProcessor;
import com.github.dtprj.dongting.raft.rpc.TransferLeaderProcessor;
import com.github.dtprj.dongting.raft.rpc.VoteProcessor;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class RaftServer extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftServer.class);
    private final RaftFactory raftFactory;

    private final NioServer replicateNioServer;
    private final NioClient replicateNioClient;
    private final NioServer serviceNioServer;

    private final ConcurrentHashMap<Integer, RaftGroupImpl> raftGroups = new ConcurrentHashMap<>();

    private final RaftServerConfig serverConfig;

    private final NodeManager nodeManager;

    private final PendingStat serverStat = new PendingStat();

    // indicate each group has enough members (>= elect quorum) raft ping ok
    private final CompletableFuture<Void> allMemberReadyFuture = new CompletableFuture<>();

    // indicate each group has applied groupReadyIndex, and serviceNioServer started
    private final CompletableFuture<Void> allGroupReadyFuture = new CompletableFuture<>();

    private final Set<RaftSequenceProcessor<?>> raftSequenceProcessors = new HashSet<>();

    public RaftServer(RaftServerConfig serverConfig, List<RaftGroupConfig> groupConfig, RaftFactory raftFactory) {
        Objects.requireNonNull(serverConfig);
        Objects.requireNonNull(groupConfig);
        Objects.requireNonNull(raftFactory);
        this.serverConfig = serverConfig;
        this.raftFactory = raftFactory;

        Objects.requireNonNull(serverConfig.getServers());
        DtUtil.checkPositive(serverConfig.getNodeId(), "id");
        DtUtil.checkPositive(serverConfig.getReplicatePort(), "replicatePort");

        List<RaftNode> allRaftServers = RaftNode.parseServers(serverConfig.getServers());
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
        repClientConfig.setName("RaftRepClient" + serverConfig.getNodeId());
        repClientConfig.setConnectRetryIntervals(null); //use node ping
        setupNioConfig(repClientConfig);
        customReplicateNioClient(repClientConfig);
        replicateNioClient = new NioClient(repClientConfig);

        nodeManager = new NodeManager(serverConfig, allRaftServers, replicateNioClient,
                RaftUtil.getElectQuorum(allRaftServers.size()));

        NioServerConfig repServerConfig = new NioServerConfig();
        repServerConfig.setPort(serverConfig.getReplicatePort());
        repServerConfig.setName("RaftRepServer" + serverConfig.getNodeId());
        repServerConfig.setBizThreads(0);
        // use multi io threads
        setupNioConfig(repServerConfig);
        customReplicateNioServer(repServerConfig);
        replicateNioServer = new NioServer(repServerConfig);

        replicateNioServer.register(Commands.NODE_PING, new NodePingProcessor(serverConfig.getNodeId(), nodeManager));
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_PING, new RaftPingProcessor(this));
        AppendProcessor appendProcessor = new AppendProcessor(this);
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_APPEND_ENTRIES, appendProcessor);
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_INSTALL_SNAPSHOT, appendProcessor);
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_REQUEST_VOTE, new VoteProcessor(this));
        replicateNioServer.register(Commands.RAFT_ADMIN_TRANSFER_LEADER, new AdminTransferLeaderProcessor(this));
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_TRANSFER_LEADER, new TransferLeaderProcessor(this));
        QueryStatusProcessor queryStatusProcessor = new QueryStatusProcessor(this);
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_QUERY_STATUS, queryStatusProcessor);
        AdminConfigChangeProcessor adminConfigChangeProcessor = new AdminConfigChangeProcessor(this);
        replicateNioServer.register(Commands.RAFT_ADMIN_PREPARE_CHANGE, adminConfigChangeProcessor);
        replicateNioServer.register(Commands.RAFT_ADMIN_COMMIT_CHANGE, adminConfigChangeProcessor);
        replicateNioServer.register(Commands.RAFT_ADMIN_ABORT_CHANGE, adminConfigChangeProcessor);
        AdminGroupAndNodeProcessor adminGroupAndNodeProcessor = new AdminGroupAndNodeProcessor(this, raftFactory);
        replicateNioServer.register(Commands.RAFT_ADMIN_ADD_GROUP, adminGroupAndNodeProcessor);
        replicateNioServer.register(Commands.RAFT_ADMIN_REMOVE_GROUP, adminGroupAndNodeProcessor);
        replicateNioServer.register(Commands.RAFT_ADMIN_ADD_NODE, adminGroupAndNodeProcessor);
        replicateNioServer.register(Commands.RAFT_ADMIN_REMOVE_NODE, adminGroupAndNodeProcessor);

        if (serverConfig.getServicePort() > 0) {
            NioServerConfig serviceServerConfig = new NioServerConfig();
            serviceServerConfig.setPort(serverConfig.getServicePort());
            serviceServerConfig.setName("RaftServiceServer" + serverConfig.getNodeId());
            serviceServerConfig.setBizThreads(0);
            // use multi io threads
            serviceServerConfig.setDecodeContextFactory(DecodeContextEx::new);
            customServiceNioServer(serviceServerConfig);
            serviceNioServer = new NioServer(serviceServerConfig);
            addRaftGroupProcessor(serviceNioServer, Commands.RAFT_QUERY_STATUS, queryStatusProcessor);
        } else {
            serviceNioServer = null;
        }
        createRaftGroups(serverConfig, groupConfig, allNodeIds);

        allMemberReadyFuture.whenComplete(this::afterAllMemberReady);
    }

    private void addRaftGroupProcessor(NioServer nioServer, int command, RaftSequenceProcessor<?> processor) {
        nioServer.register(command, processor);
        raftSequenceProcessors.add(processor);
    }

    private void setupNioConfig(NioConfig nc) {
        nc.setFinishPendingImmediatelyWhenChannelClose(true);
        nc.setMaxOutRequests(0);
        nc.setMaxOutBytes(0);
        nc.setMaxInRequests(0);
        nc.setMaxInBytes(0);
        nc.setMaxBodySize(Integer.MAX_VALUE);
        nc.setMaxPacketSize(Integer.MAX_VALUE);
        nc.setDecodeContextFactory(DecodeContextEx::new);
    }

    protected void customReplicateNioClient(@SuppressWarnings("unused") NioClientConfig c) {
    }

    protected void customReplicateNioServer(@SuppressWarnings("unused") NioServerConfig c) {
    }

    protected void customServiceNioServer(@SuppressWarnings("unused") NioServerConfig c) {
    }

    private void createRaftGroups(RaftServerConfig serverConfig,
                                  List<RaftGroupConfig> groupConfig, HashSet<Integer> allNodeIds) {
        for (RaftGroupConfig rgc : groupConfig) {
            if (raftGroups.get(rgc.getGroupId()) != null) {
                throw new IllegalArgumentException("duplicate group id: " + rgc.getGroupId());
            }
            RaftGroupImpl g = createRaftGroup(serverConfig, allNodeIds, rgc);
            raftGroups.put(rgc.getGroupId(), g);
        }
    }

    private RaftGroupImpl createRaftGroup(RaftServerConfig serverConfig, Set<Integer> allNodeIds, RaftGroupConfig rgc) {
        Objects.requireNonNull(rgc.getNodeIdOfMembers());

        GroupComponents gc = new GroupComponents();

        Set<Integer> nodeIdOfMembers = parseMemberIds(allNodeIds, rgc.getNodeIdOfMembers(), rgc.getGroupId());
        if (nodeIdOfMembers.isEmpty()) {
            throw new IllegalArgumentException("no member in group: " + rgc.getGroupId());
        }

        Set<Integer> nodeIdOfObservers;
        if (rgc.getNodeIdOfObservers() != null && !rgc.getNodeIdOfObservers().trim().isEmpty()) {
            nodeIdOfObservers = parseMemberIds(allNodeIds, rgc.getNodeIdOfObservers(), rgc.getGroupId());
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
        if (!isMember && !isObserver) {
            log.warn("node {} is not member or observer of group {}", serverConfig.getNodeId(), rgc.getGroupId());
        }

        Dispatcher dispatcher = raftFactory.createDispatcher(rgc);
        FiberGroup fiberGroup = new FiberGroup("group-" + rgc.getGroupId(), dispatcher);
        RaftStatusImpl raftStatus = new RaftStatusImpl(fiberGroup.getDispatcher().getTs());
        raftStatus.setTailCache(new TailCache(rgc, raftStatus));
        raftStatus.setNodeIdOfMembers(nodeIdOfMembers);
        raftStatus.setNodeIdOfObservers(nodeIdOfObservers);
        raftStatus.setGroupId(rgc.getGroupId());

        RaftGroupConfigEx rgcEx = createGroupConfigEx(rgc, raftStatus, fiberGroup);

        gc.setServerConfig(serverConfig);
        gc.setGroupConfig(rgcEx);
        gc.setRaftStatus(raftStatus);
        gc.setFiberGroup(fiberGroup);

        StateMachine stateMachine = raftFactory.createStateMachine(rgcEx);
        StatusManager statusManager = new StatusManager(rgcEx);
        RaftLog raftLog = raftFactory.createRaftLog(rgcEx, statusManager, stateMachine);

        ApplyManager applyManager = new ApplyManager(gc);
        CommitManager commitManager = new CommitManager(gc);
        ReplicateManager replicateManager = new ReplicateManager(replicateNioClient, gc);
        MemberManager memberManager = new MemberManager(replicateNioClient, gc);
        LinearTaskRunner linearTaskRunner = new LinearTaskRunner(gc);
        VoteManager voteManager = new VoteManager(replicateNioClient, gc);

        gc.setRaftLog(raftLog);
        gc.setStateMachine(stateMachine);
        gc.setMemberManager(memberManager);
        gc.setReplicateManager(replicateManager);
        gc.setVoteManager(voteManager);
        gc.setCommitManager(commitManager);
        gc.setApplyManager(applyManager);
        gc.setNodeManager(nodeManager);
        gc.setServerStat(serverStat);
        gc.setSnapshotManager(raftFactory.createSnapshotManager(rgcEx, stateMachine));
        gc.setStatusManager(statusManager);
        gc.setLinearTaskRunner(linearTaskRunner);

        applyManager.postInit();
        commitManager.postInit();
        replicateManager.postInit();
        memberManager.postInit();
        voteManager.postInit();
        linearTaskRunner.postInit();

        for (RaftSequenceProcessor<?> processor : raftSequenceProcessors) {
            FiberChannel<Object> channel = fiberGroup.newChannel();
            gc.getProcessorChannels().put(processor.getTypeId(), channel);
        }

        return new RaftGroupImpl(gc);
    }

    private RaftGroupConfigEx createGroupConfigEx(RaftGroupConfig rgc, RaftStatusImpl raftStatus,
                                                  FiberGroup fiberGroup) {
        RaftGroupConfigEx rgcEx = (RaftGroupConfigEx) rgc;
        rgcEx.setTs(raftStatus.getTs());
        rgcEx.setRaftStatus(raftStatus);
        rgcEx.setBlockIoExecutor(raftFactory.createBlockIoExecutor(serverConfig));
        rgcEx.setFiberGroup(fiberGroup);
        return rgcEx;
    }

    private static Set<Integer> parseMemberIds(Set<Integer> allNodeIds, String str, int groupId) {
        Set<Integer> s = new HashSet<>();
        String[] membersStr = str.split(",");
        for (String idStr : membersStr) {
            int id = Integer.parseInt(idStr.trim());
            if (!allNodeIds.contains(id)) {
                throw new IllegalArgumentException("member id " + id + " not in server list: groupId=" + groupId);
            }
            if (!s.add(id)) {
                throw new IllegalArgumentException("duplicated raft member id " + id + ".  groupId=" + groupId);
            }
        }
        return s;
    }

    @Override
    protected void doStart() {
        try {
            // start replicate server and client
            replicateNioServer.start();
            replicateNioClient.start();
            replicateNioClient.waitStart();

            // sync but should complete soon
            nodeManager.initNodes(raftGroups);

            // start all fiber group
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
            raftGroups.forEach((groupId, g) -> {
                GroupComponents gc = g.getGroupComponents();
                // nodeManager.getAllNodesEx() is not thread safe
                gc.getMemberManager().init();

                FiberGroup fg = gc.getFiberGroup();
                raftFactory.startDispatcher(fg.getDispatcher());
                CompletableFuture<Void> f = fg.getDispatcher().startGroup(fg);
                futures.add(f);
            });
            // should complete soon, so we wait here
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);

            // init raft log and state machine, async in raft thread
            futures.clear();
            raftGroups.forEach((groupId, g) -> {
                initRaftGroup(g);
                futures.add(g.getGroupComponents().getRaftStatus().getInitFuture());
            });
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((v, ex) -> {
                if (ex != null) {
                    allMemberReadyFuture.completeExceptionally(ex);
                } else if (checkStartStatus(allMemberReadyFuture)) {
                    startNodePing();
                }
            });
        } catch (Exception e) {
            log.error("start raft server failed", e);
            throw new RaftException(e);
        }
    }

    private boolean checkStartStatus(CompletableFuture<Void> f) {
        if (status > STATUS_RUNNING) {
            f.completeExceptionally(new IllegalStateException("server is not running: " + status));
            return false;
        }
        return true;
    }

    private void initRaftGroup(RaftGroupImpl g) {
        GroupComponents gc = g.getGroupComponents();
        InitFiberFrame initFiberFrame = new InitFiberFrame(gc, raftSequenceProcessors);
        Fiber initFiber = new Fiber("init-raft-group-" + g.getGroupId(),
                gc.getFiberGroup(), initFiberFrame);
        if (!gc.getFiberGroup().fireFiber(initFiber)) {
            throw new RaftException("fire init fiber failed");
        }
    }

    private void startNodePing() {
        try {
            nodeManager.start();
            nodeManager.getNodePingReadyFuture().whenComplete((v, ex) -> {
                if (ex != null) {
                    allMemberReadyFuture.completeExceptionally(ex);
                } else if (checkStartStatus(allMemberReadyFuture)) {
                    startMemberPing();
                }
            });
        } catch (Exception e) {
            log.error("start node manager failed", e);
            allMemberReadyFuture.completeExceptionally(e);
        }
    }

    private void startMemberPing() {
        try {
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
            raftGroups.forEach((groupId, g) -> futures.add(startMemberPing(g)));

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((v, ex) -> {
                if (ex != null) {
                    allMemberReadyFuture.completeExceptionally(ex);
                } else if (checkStartStatus(allMemberReadyFuture)) {
                    allMemberReadyFuture.complete(null);
                }
            });
        } catch (Exception e) {
            log.error("start raft groups failed", e);
            allMemberReadyFuture.completeExceptionally(e);
        }
    }

    private void afterAllMemberReady(Void unused, Throwable ex) {
        if (ex != null) {
            allGroupReadyFuture.completeExceptionally(ex);
            return;
        }
        try {
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
            DtTime deadline = new DtTime(1000, TimeUnit.DAYS);
            raftGroups.forEach((groupId, g) -> {
                ShareStatus ss = g.getGroupComponents().getRaftStatus().getShareStatus();
                if (!ss.groupReady) {
                    futures.add(g.getGroupComponents().getApplyManager()
                            .addToWaitReadyQueue(deadline).thenApply(idx -> null));
                }
            });

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((v, ex2) -> {
                if (ex2 != null) {
                    allGroupReadyFuture.completeExceptionally(ex);
                } else if (checkStartStatus(allGroupReadyFuture)) {
                    try {
                        if (serviceNioServer != null) {
                            serviceNioServer.start();
                        }
                        allGroupReadyFuture.complete(null);
                    } catch (Exception serviceNioServerStartEx) {
                        allGroupReadyFuture.completeExceptionally(serviceNioServerStartEx);
                    }
                }
            });
        } catch (Exception e) {
            log.error("start raft groups failed", e);
            allGroupReadyFuture.completeExceptionally(e);
        }
    }

    private CompletableFuture<Void> startMemberPing(RaftGroupImpl g) {
        GroupComponents gc = g.getGroupComponents();
        if (!gc.getFiberGroup().fireFiber(gc.getMemberManager().createRaftPingFiber())) {
            return CompletableFuture.failedFuture(new RaftException("fire raft ping fiber failed"));
        }
        return gc.getMemberManager().getPingReadyFuture();
    }

    @SuppressWarnings("unused")
    public CompletableFuture<Void> getAllMemberReadyFuture() {
        return allMemberReadyFuture;
    }

    public CompletableFuture<Void> getAllGroupReadyFuture() {
        return allGroupReadyFuture;
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
        try {
            if (serviceNioServer != null) {
                serviceNioServer.stop(timeout, true);
            }
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
            raftGroups.forEach((groupId, g) -> futures.add(stopGroup(g, timeout,
                    g.getGroupComponents().getGroupConfig().isSaveSnapshotWhenClose())));
            nodeManager.stop(timeout, true);

            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(timeout.rest(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                DtUtil.restoreInterruptStatus();
                throw new RaftException(e);
            } catch (Exception e) {
                throw new RaftException(e);
            } finally {
                if (replicateNioServer != null) {
                    replicateNioServer.stop(timeout);
                }
                if (replicateNioClient != null) {
                    replicateNioClient.stop(timeout, true);
                }
                raftFactory.shutdownBlockIoExecutor();
            }
        } catch (RuntimeException | Error e) {
            log.error("stop raft server failed", e);
            throw e;
        }
    }

    private CompletableFuture<Void> stopGroup(RaftGroupImpl g, DtTime timeout, boolean saveSnapshot) {
        if (g.getShutdownFuture() != null) {
            return g.getShutdownFuture();
        }
        FiberGroup fiberGroup = g.getFiberGroup();
        GroupComponents gc = g.getGroupComponents();
        fiberGroup.fireFiber("shutdown" + g.getGroupId(), new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                fiberGroup.requestShutdown();
                return fiberGroup.getShouldStopCondition().await(this::afterShouldShutdown);
            }

            private FrameCallResult afterShouldShutdown(Void v) {
                FiberFuture<Long> f;
                if (saveSnapshot) {
                    f = gc.getSnapshotManager().saveSnapshot();
                } else {
                    f = FiberFuture.completedFuture(getFiberGroup(), 0L);
                }
                return f.await(this::afterSaveSnapshot);
            }

            private FrameCallResult afterSaveSnapshot(Long notUsed) {
                gc.getApplyManager().shutdown(timeout);
                return gc.getRaftLog().close().await(this::afterRaftLogClose);
            }

            private FrameCallResult afterRaftLogClose(Void unused) {
                g.getGroupComponents().getRaftStatus().getTailCache().cleanAll();
                return g.getGroupComponents().getStatusManager().close().await(this::justReturn);
            }
        });

        // the group shutdown is not finished, but it's ok to call afterGroupShutdown(to shutdown dispatcher)
        raftFactory.stopDispatcher(fiberGroup.getDispatcher(), timeout);

        CompletableFuture<Void> f = g.getFiberGroup().getShutdownFuture().thenRun(() -> raftGroups.remove(g.getGroupId()));
        g.setShutdownFuture(f);
        return f;
    }

    /**
     * ADMIN API. This method is idempotent and may block. When complete the new node is connected.
     * If the node is already in node list and connected, the future complete normally immediately.
     */
    public CompletableFuture<Void> addNode(RaftNode node) {
        return nodeManager.addNode(node).thenApply(o -> null);
    }

    /**
     * ADMIN API. This method is idempotent and may block. If the node is node in node list, complete normally immediately.
     * If the reference count of the node is not 0, the future complete exceptionally.
     */
    public CompletableFuture<Void> removeNode(int nodeId) {
        return nodeManager.removeNode(nodeId);
    }

    /**
     * ADMIN API. This method is NOT idempotent.
     * <p>
     * This method may block (before return future).
     * After get the CompletableFuture, user should wait on the future to ensure raft group is initialized.
     */
    public CompletableFuture<Void> addGroup(RaftGroupConfig groupConfig) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        Runnable r = () -> {
            try {
                if (status != STATUS_RUNNING) {
                    f.completeExceptionally(new RaftException("raft server is not running"));
                    return;
                }
                if (raftGroups.get(groupConfig.getGroupId()) != null) {
                    f.completeExceptionally(new RaftException("group already exist: " + groupConfig.getGroupId()));
                    return;
                }
                RaftGroupImpl g = createRaftGroup(serverConfig, nodeManager.getAllNodeIds(), groupConfig);
                g.getGroupComponents().getMemberManager().init();

                GroupComponents gc = g.getGroupComponents();
                FiberGroup fg = gc.getFiberGroup();
                raftFactory.startDispatcher(fg.getDispatcher());
                fg.getDispatcher().startGroup(fg).get(3, TimeUnit.SECONDS);

                raftGroups.put(groupConfig.getGroupId(), g);
                initRaftGroup(g);
                RaftStatusImpl raftStatus = g.getGroupComponents().getRaftStatus();
                raftStatus.getInitFuture().whenComplete((v, ex) -> {
                    if (ex != null) {
                        f.completeExceptionally(ex);
                    } else {
                        f.complete(null);
                    }
                });
            } catch (InterruptedException e) {
                DtUtil.restoreInterruptStatus();
                f.completeExceptionally(e);
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        };
        DtUtil.SCHEDULED_SERVICE.execute(r);
        return f;
    }

    /**
     * ADMIN API. This method is idempotent.
     */
    public CompletableFuture<Void> removeGroup(int groupId, boolean saveSnapshot, DtTime shutdownTimeout) {
        RaftGroupImpl g = raftGroups.get(groupId);
        if (g == null) {
            log.warn("group {} not exist", groupId);
            return CompletableFuture.completedFuture(null);
        } else {
            if (status != STATUS_RUNNING) {
                return CompletableFuture.failedFuture(new RaftException("raft server is not running"));
            } else {
                return stopGroup(g, shutdownTimeout, saveSnapshot);
            }
        }
    }

    public RaftGroup getRaftGroup(int groupId) {
        return raftGroups.get(groupId);
    }

    public NioServer getServiceNioServer() {
        return serviceNioServer;
    }

    public RaftServerConfig getServerConfig() {
        return serverConfig;
    }
}
