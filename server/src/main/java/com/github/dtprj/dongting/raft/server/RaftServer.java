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
import com.github.dtprj.dongting.raft.NoSuchGroupException;
import com.github.dtprj.dongting.raft.QueryStatusResp;
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
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftShareStatus;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.ReplicateManager;
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
import java.util.function.Consumer;

/**
 * @author huangli
 */
public class RaftServer extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftServer.class);
    private final RaftFactory raftFactory;

    private final NioServer nioServer;
    private final NioClient nioClient;

    private volatile boolean groupReady;

    private final ConcurrentHashMap<Integer, RaftGroupImpl> raftGroups = new ConcurrentHashMap<>();

    private final RaftServerConfig serverConfig;

    private final NodeManager nodeManager;

    // indicate each group has enough members (>= elect quorum) raft ping ok
    private final CompletableFuture<Void> allMemberReadyFuture = new CompletableFuture<>();

    // indicate each group has applied groupReadyIndex, and serviceNioServer started
    private final CompletableFuture<Void> allGroupReadyFuture = new CompletableFuture<>();

    private final Set<RaftSequenceProcessor<?>> raftSequenceProcessors = new HashSet<>();

    final Consumer<RaftGroupImpl> groupCustomizer;

    public RaftServer(RaftServerConfig serverConfig, List<RaftGroupConfig> groupConfig, RaftFactory raftFactory) {
        this(serverConfig, groupConfig, raftFactory, null);
    }

    RaftServer(RaftServerConfig serverConfig, List<RaftGroupConfig> groupConfig, RaftFactory raftFactory,
               Consumer<RaftGroupImpl> groupCustomizer) {
        Objects.requireNonNull(serverConfig);
        Objects.requireNonNull(groupConfig);
        Objects.requireNonNull(raftFactory);
        this.serverConfig = serverConfig;
        this.raftFactory = raftFactory;
        this.groupCustomizer = groupCustomizer;

        Objects.requireNonNull(serverConfig.servers);
        DtUtil.checkPositive(serverConfig.nodeId, "id");
        DtUtil.checkPositive(serverConfig.replicatePort, "replicatePort");

        List<RaftNode> allRaftServers = RaftNode.parseServers(serverConfig.servers);
        HashSet<Integer> allNodeIds = new HashSet<>();
        HashSet<HostPort> allNodeHosts = new HashSet<>();
        for (RaftNode rn : allRaftServers) {
            if (!allNodeIds.add(rn.nodeId)) {
                throw new IllegalArgumentException("duplicate server id: " + rn.nodeId);
            }
            if (!allNodeHosts.add(rn.hostPort)) {
                throw new IllegalArgumentException("duplicate server host: " + rn.hostPort);
            }
        }
        if (!allNodeIds.contains(serverConfig.nodeId)) {
            throw new IllegalArgumentException("self id not found in servers list: " + serverConfig.nodeId);
        }

        NioClientConfig repClientConfig = new NioClientConfig();
        repClientConfig.name = "RaftRepClient" + serverConfig.nodeId;
        repClientConfig.connectRetryIntervals = null; //use node ping
        setupNioConfig(repClientConfig);
        customReplicateNioClient(repClientConfig);
        nioClient = new NioClient(repClientConfig);

        NioServerConfig repServerConfig = new NioServerConfig();
        if (serverConfig.servicePort > 0) {
            repServerConfig.ports = new int[]{serverConfig.replicatePort, serverConfig.servicePort};
        } else {
            repServerConfig.port = serverConfig.replicatePort;
        }
        repServerConfig.name = "RaftRepServer" + serverConfig.nodeId;
        repServerConfig.bizThreads = 1;
        setupNioConfig(repServerConfig);
        customReplicateNioServer(repServerConfig);
        nioServer = new NioServer(repServerConfig);

        nodeManager = new NodeManager(serverConfig, allRaftServers, nioClient,
                RaftUtil.getElectQuorum(allRaftServers.size()), nioServer);

        nioServer.register(Commands.NODE_PING, new NodePingProcessor(nodeManager, serverConfig));
        addRaftGroupProcessor(nioServer, Commands.RAFT_PING, new RaftPingProcessor(this));
        AppendProcessor appendProcessor = new AppendProcessor(this);
        addRaftGroupProcessor(nioServer, Commands.RAFT_APPEND_ENTRIES, appendProcessor);
        addRaftGroupProcessor(nioServer, Commands.RAFT_INSTALL_SNAPSHOT, appendProcessor);
        addRaftGroupProcessor(nioServer, Commands.RAFT_REQUEST_VOTE, new VoteProcessor(this));
        nioServer.register(Commands.RAFT_ADMIN_TRANSFER_LEADER, new AdminTransferLeaderProcessor(this));
        addRaftGroupProcessor(nioServer, Commands.RAFT_TRANSFER_LEADER, new TransferLeaderProcessor(this));
        QueryStatusProcessor queryStatusProcessor = new QueryStatusProcessor(this);
        addRaftGroupProcessor(nioServer, Commands.RAFT_QUERY_STATUS, queryStatusProcessor);
        AdminConfigChangeProcessor adminConfigChangeProcessor = new AdminConfigChangeProcessor(this);
        nioServer.register(Commands.RAFT_ADMIN_PREPARE_CHANGE, adminConfigChangeProcessor);
        nioServer.register(Commands.RAFT_ADMIN_COMMIT_CHANGE, adminConfigChangeProcessor);
        nioServer.register(Commands.RAFT_ADMIN_ABORT_CHANGE, adminConfigChangeProcessor);
        AdminGroupAndNodeProcessor adminGroupAndNodeProcessor = new AdminGroupAndNodeProcessor(this, nodeManager, raftFactory);
        nioServer.register(Commands.RAFT_ADMIN_ADD_GROUP, adminGroupAndNodeProcessor);
        nioServer.register(Commands.RAFT_ADMIN_REMOVE_GROUP, adminGroupAndNodeProcessor);
        nioServer.register(Commands.RAFT_ADMIN_ADD_NODE, adminGroupAndNodeProcessor);
        nioServer.register(Commands.RAFT_ADMIN_REMOVE_NODE, adminGroupAndNodeProcessor);
        nioServer.register(Commands.RAFT_ADMIN_LIST_NODES, adminGroupAndNodeProcessor);
        nioServer.register(Commands.RAFT_ADMIN_LIST_GROUPS, adminGroupAndNodeProcessor);

        createRaftGroups(serverConfig, groupConfig, allNodeIds);

        allMemberReadyFuture.whenComplete(this::afterAllMemberReady);
    }

    private void addRaftGroupProcessor(NioServer nioServer, int command, RaftSequenceProcessor<?> processor) {
        // use io executor
        nioServer.register(command, processor, null);
        raftSequenceProcessors.add(processor);
    }

    private void setupNioConfig(NioConfig nc) {
        nc.maxOutRequests = 0;
        nc.maxOutBytes = 0;
        nc.maxInRequests = 0;
        nc.maxInBytes = 0;
        nc.maxBodySize = Integer.MAX_VALUE;
        nc.maxPacketSize = Integer.MAX_VALUE;
        nc.decodeContextFactory = DecodeContextEx::new;
    }

    protected void customReplicateNioClient(@SuppressWarnings("unused") NioClientConfig c) {
    }

    protected void customReplicateNioServer(@SuppressWarnings("unused") NioServerConfig c) {
    }

    private void createRaftGroups(RaftServerConfig serverConfig,
                                  List<RaftGroupConfig> groupConfig, HashSet<Integer> allNodeIds) {
        for (RaftGroupConfig rgc : groupConfig) {
            if (raftGroups.get(rgc.groupId) != null) {
                throw new IllegalArgumentException("duplicate group id: " + rgc.groupId);
            }
            RaftGroupImpl g = createRaftGroup(serverConfig, allNodeIds, rgc);
            raftGroups.put(rgc.groupId, g);
        }
    }

    private RaftGroupImpl createRaftGroup(RaftServerConfig serverConfig, Set<Integer> allNodeIds, RaftGroupConfig rgc) {
        Objects.requireNonNull(rgc.nodeIdOfMembers);

        GroupComponents gc = new GroupComponents();

        Set<Integer> nodeIdOfMembers = parseMemberIds(allNodeIds, rgc.nodeIdOfMembers, rgc.groupId);
        if (nodeIdOfMembers.isEmpty()) {
            throw new IllegalArgumentException("no member in group: " + rgc.groupId);
        }

        Set<Integer> nodeIdOfObservers;
        if (rgc.nodeIdOfObservers != null && !rgc.nodeIdOfObservers.trim().isEmpty()) {
            nodeIdOfObservers = parseMemberIds(allNodeIds, rgc.nodeIdOfObservers, rgc.groupId);
            for (int id : nodeIdOfMembers) {
                if (nodeIdOfObservers.contains(id)) {
                    throw new IllegalArgumentException("member and observer has same node: " + id);
                }
            }
        } else {
            nodeIdOfObservers = Collections.emptySet();
        }

        boolean isMember = nodeIdOfMembers.contains(serverConfig.nodeId);
        boolean isObserver = nodeIdOfObservers.contains(serverConfig.nodeId);
        if (!isMember && !isObserver) {
            log.warn("node {} is not member or observer of group {}", serverConfig.nodeId, rgc.groupId);
        }

        Dispatcher dispatcher = raftFactory.createDispatcher(serverConfig, rgc);
        FiberGroup fiberGroup = new FiberGroup("group-" + rgc.groupId, dispatcher);
        RaftStatusImpl raftStatus = new RaftStatusImpl(rgc.groupId, fiberGroup.dispatcher.ts);
        raftStatus.nioServer = nioServer;
        raftStatus.tailCache = new TailCache(rgc, raftStatus);
        raftStatus.nodeIdOfMembers = nodeIdOfMembers;
        raftStatus.nodeIdOfObservers = nodeIdOfObservers;

        RaftGroupConfigEx rgcEx = createGroupConfigEx(rgc, raftStatus, fiberGroup);

        gc.serverConfig = serverConfig;
        gc.groupConfig = rgcEx;
        gc.raftStatus = raftStatus;
        gc.fiberGroup = fiberGroup;

        StateMachine stateMachine = raftFactory.createStateMachine(rgcEx);
        StatusManager statusManager = new StatusManager(rgcEx);
        RaftLog raftLog = raftFactory.createRaftLog(rgcEx, statusManager, stateMachine);

        ApplyManager applyManager = new ApplyManager(gc);
        CommitManager commitManager = new CommitManager(gc);
        ReplicateManager replicateManager = new ReplicateManager(nioClient, gc);
        MemberManager memberManager = new MemberManager(nioClient, gc);
        LinearTaskRunner linearTaskRunner = new LinearTaskRunner(gc);
        VoteManager voteManager = new VoteManager(nioClient, gc);

        gc.raftLog = raftLog;
        gc.stateMachine = stateMachine;
        gc.memberManager = memberManager;
        gc.replicateManager = replicateManager;
        gc.voteManager = voteManager;
        gc.commitManager = commitManager;
        gc.applyManager = applyManager;
        gc.nodeManager = nodeManager;
        gc.snapshotManager = raftFactory.createSnapshotManager(rgcEx, stateMachine, raftLog);
        gc.statusManager = statusManager;
        gc.linearTaskRunner = linearTaskRunner;

        applyManager.postInit();
        commitManager.postInit();
        replicateManager.postInit();
        memberManager.postInit();
        voteManager.postInit();
        linearTaskRunner.postInit();

        RaftGroupImpl g = new RaftGroupImpl(gc);
        if (groupCustomizer != null) {
            groupCustomizer.accept(g);
        }
        stateMachine.setRaftGroup(g);
        return g;
    }

    private RaftGroupConfigEx createGroupConfigEx(RaftGroupConfig rgc, RaftStatusImpl raftStatus,
                                                  FiberGroup fiberGroup) {
        RaftGroupConfigEx rgcEx = (RaftGroupConfigEx) rgc;
        rgcEx.ts = raftStatus.ts;
        rgcEx.raftStatus = raftStatus;
        rgcEx.fiberGroup = fiberGroup;
        rgcEx.blockIoExecutor = raftFactory.createBlockIoExecutor(serverConfig, rgcEx);
        rgcEx.raftServer = this;
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
            nioServer.start();
            nioClient.start(); // has no servers now

            // sync but should complete soon
            nodeManager.initNodes(raftGroups);

            // start all fiber group
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
            raftGroups.forEach((groupId, g) -> {
                GroupComponents gc = g.groupComponents;
                // nodeManager.getAllNodesEx() is not thread safe
                gc.memberManager.init();

                FiberGroup fg = gc.fiberGroup;
                raftFactory.startDispatcher(fg.dispatcher);
                CompletableFuture<Void> f = fg.dispatcher.startGroup(fg);
                futures.add(f);
            });
            // should complete soon, so we wait here
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);

            // init raft log and state machine, async in raft thread
            futures.clear();
            raftGroups.forEach((groupId, g) -> {
                initRaftGroup(g);
                futures.add(g.groupComponents.raftStatus.initFuture);
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
        GroupComponents gc = g.groupComponents;
        InitFiberFrame initFiberFrame = new InitFiberFrame(gc, raftSequenceProcessors);
        Fiber initFiber = new Fiber("init-raft-group-" + g.getGroupId(),
                gc.fiberGroup, initFiberFrame);
        if (!gc.fiberGroup.fireFiber(initFiber)) {
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
            log.info("all group member check ready");
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
            DtTime deadline = new DtTime(1000, TimeUnit.DAYS);
            raftGroups.forEach((groupId, g) -> {
                RaftShareStatus ss = g.groupComponents.raftStatus.getShareStatus();
                if (!ss.groupReady && ss.role != RaftRole.none) {
                    futures.add(g.groupComponents.applyManager
                            .addToWaitReadyQueue(deadline).thenApply(idx -> null));
                }
            });

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((v, ex2) -> {
                if (ex2 != null) {
                    allGroupReadyFuture.completeExceptionally(ex);
                } else if (checkStartStatus(allGroupReadyFuture)) {
                    groupReady = true;
                    log.info("all group ready");
                    allGroupReadyFuture.complete(null);
                }
            });
        } catch (Exception e) {
            log.error("start raft groups failed", e);
            allGroupReadyFuture.completeExceptionally(e);
        }
    }

    private CompletableFuture<Void> startMemberPing(RaftGroupImpl g) {
        GroupComponents gc = g.groupComponents;
        if (!gc.fiberGroup.fireFiber(gc.memberManager.createRaftPingFiber())) {
            return CompletableFuture.failedFuture(new RaftException("fire raft ping fiber failed"));
        }
        return gc.memberManager.getPingReadyFuture();
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
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
            raftGroups.forEach((groupId, g) -> {
                stopGroup(g, timeout, g.groupComponents.groupConfig.saveSnapshotWhenClose);
                futures.add(g.fiberGroup.shutdownFuture);
            });

            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(timeout.rest(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new RaftException(e);
            } finally {
                nodeManager.stop(timeout, true);
                if (nioServer != null) {
                    nioServer.stop(timeout);
                }
                if (nioClient != null) {
                    nioClient.stop(timeout, true);
                }
            }
        } catch (RuntimeException | Error e) {
            log.error("stop raft server failed", e);
            throw e;
        }
    }

    private void stopGroup(RaftGroupImpl g, DtTime timeout, boolean saveSnapshot) {
        FiberGroup fiberGroup = g.fiberGroup;
        GroupComponents gc = g.groupComponents;
        fiberGroup.fireFiber("shutdown" + g.getGroupId(), new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (isGroupShouldStopPlain()) {
                    return Fiber.frameReturn();
                }

                // fireFiber run in current thread, so shouldStop set immediately here
                fiberGroup.requestShutdown();

                FiberFuture<Long> f;
                if (saveSnapshot) {
                    f = gc.snapshotManager.saveSnapshot();
                } else {
                    f = FiberFuture.completedFuture(getFiberGroup(), 0L);
                }
                return f.await(this::afterSaveSnapshot);
            }

            private FrameCallResult afterSaveSnapshot(Long notUsed) {
                gc.applyManager.shutdown(timeout);
                return gc.raftLog.close().await(this::afterRaftLogClose);
            }

            private FrameCallResult afterRaftLogClose(Void unused) {
                g.groupComponents.raftStatus.tailCache.cleanAll();
                return g.groupComponents.statusManager.close().await(this::afterStatusManagerClose);
            }

            private FrameCallResult afterStatusManagerClose(Void unused) {
                raftFactory.shutdownBlockIoExecutor(serverConfig, gc.groupConfig,
                        gc.groupConfig.blockIoExecutor);
                return Fiber.frameReturn();
            }
        });

        // the group shutdown is not finished, but it's ok to call afterGroupShutdown(to shutdown dispatcher)
        raftFactory.stopDispatcher(fiberGroup.dispatcher, timeout);

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
     * The return future complete when the group is added, but the group may not be ready,
     * you should check the group status to make sure it's ready.
     *
     * @see #queryRaftGroupStatus(int)
     */
    public CompletableFuture<Void> addGroup(RaftGroupConfig groupConfig) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        try {
            if (status != STATUS_RUNNING) {
                f.completeExceptionally(new RaftException("raft server is not running"));
                return f;
            }
            if (raftGroups.get(groupConfig.groupId) != null) {
                f.completeExceptionally(new RaftException("group already exist: " + groupConfig.groupId));
                return f;
            }
            RaftGroupImpl g;
            nodeManager.getLock().lock();
            try {
                g = createRaftGroup(serverConfig, nodeManager.getAllNodeIdsInLock(), groupConfig);
                nodeManager.processUseCountForGroupInLock(g, true);
            } finally {
                nodeManager.getLock().unlock();
            }
            raftGroups.put(groupConfig.groupId, g);

            g.groupComponents.memberManager.init();

            GroupComponents gc = g.groupComponents;
            FiberGroup fg = gc.fiberGroup;
            raftFactory.startDispatcher(fg.dispatcher);
            CompletableFuture<Void> startGroupFuture = fg.dispatcher.startGroup(fg);


            startGroupFuture.whenComplete((v, startEx) -> {
                if (startEx != null) {
                    f.completeExceptionally(startEx);
                } else {
                    initRaftGroup(g);
                    RaftStatusImpl raftStatus = g.groupComponents.raftStatus;
                    f.complete(null);
                    raftStatus.initFuture.whenComplete((vv, initEx) -> {
                        if (initEx != null) {
                            log.error("add group init fail", initEx);
                        } else {
                            log.info("add group init success. groupId={}", groupConfig.groupId);
                            startMemberPing(g);
                        }
                    });
                }
            });
        } catch (RuntimeException | Error e) {
            f.completeExceptionally(e);
        }
        return f;
    }

    /**
     * ADMIN API. This method is idempotent.
     * The return future complete when the group is removed, but the group may not be stopped,
     * you should check the group status to make sure it's stopped.
     *
     * @see #queryRaftGroupStatus(int)
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
                if (g.groupComponents.raftStatus.getShareStatus().shouldStop) {
                    log.warn("group {} is stopping", groupId);
                    return CompletableFuture.completedFuture(null);
                }
                stopGroup(g, shutdownTimeout, saveSnapshot);
                g.fiberGroup.shutdownFuture.thenRun(() -> {
                    boolean[] removed = new boolean[1];
                    raftGroups.compute(groupId, (k, oldGroupInMap) -> {
                        if (oldGroupInMap == g) {
                            removed[0] = true;
                            return null;
                        }
                        return oldGroupInMap;
                    });
                    if (removed[0]) {
                        nodeManager.getLock().lock();
                        try {
                            nodeManager.processUseCountForGroupInLock(g, false);
                        } finally {
                            nodeManager.getLock().unlock();
                        }
                    }
                });
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    public RaftGroup getRaftGroup(int groupId) {
        return raftGroups.get(groupId);
    }

    public RaftServerConfig getServerConfig() {
        return serverConfig;
    }

    public NioServer getNioServer() {
        return nioServer;
    }

    public NodeManager getNodeManager() {
        return nodeManager;
    }

    public boolean isGroupReady() {
        return groupReady;
    }

    public int[] getAllGroupIds() {
        return raftGroups.keySet().stream().mapToInt(Integer::intValue).toArray();
    }

    public ConcurrentHashMap<Integer, RaftGroupImpl> getRaftGroups() {
        return raftGroups;
    }

    public CompletableFuture<QueryStatusResp> queryRaftGroupStatus(int groupId) {
        RaftGroupImpl g = raftGroups.get(groupId);
        if (g == null) {
            return CompletableFuture.failedFuture(new NoSuchGroupException(groupId));
        } else {
            CompletableFuture<QueryStatusResp> f = new CompletableFuture<>();
            if (!g.fiberGroup.fireFiber("queryStatus", new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    QueryStatusResp r = QueryStatusProcessor.buildQueryStatusResp(
                            serverConfig.nodeId, g.groupComponents.raftStatus);
                    f.complete(r);
                    return Fiber.frameReturn();
                }
            })) {
                f.completeExceptionally(new NoSuchGroupException(groupId));
            }
            return f;
        }
    }

}
