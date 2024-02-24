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
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.impl.VoteManager;
import com.github.dtprj.dongting.raft.rpc.AppendProcessor;
import com.github.dtprj.dongting.raft.rpc.NodePingProcessor;
import com.github.dtprj.dongting.raft.rpc.QueryLeaderProcessor;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

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

    private final ReentrantLock changeLock = new ReentrantLock();

    private final PendingStat serverStat = new PendingStat();

    private final CompletableFuture<Void> readyFuture = new CompletableFuture<>();

    private final List<RaftSequenceProcessor<?>> raftSequenceProcessors = new ArrayList<>();

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

        nodeManager = new NodeManager(serverConfig, allRaftServers, replicateNioClient,
                RaftUtil.getElectQuorum(allRaftServers.size()));

        createRaftGroups(serverConfig, groupConfig, allNodeIds);

        NioServerConfig repServerConfig = new NioServerConfig();
        repServerConfig.setPort(serverConfig.getReplicatePort());
        repServerConfig.setName("RaftRepServer");
        repServerConfig.setBizThreads(0);
        // use multi io threads
        setupNioConfig(repServerConfig);
        replicateNioServer = new NioServer(repServerConfig);

        replicateNioServer.register(Commands.NODE_PING, new NodePingProcessor(serverConfig.getNodeId(), nodeManager.getUuid()));
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_PING, new RaftPingProcessor(this));
        AppendProcessor appendProcessor = new AppendProcessor(this);
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_APPEND_ENTRIES, appendProcessor);
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_INSTALL_SNAPSHOT, appendProcessor);
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_REQUEST_VOTE, new VoteProcessor(this));
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_LEADER_TRANSFER, new TransferLeaderProcessor(this));
        addRaftGroupProcessor(replicateNioServer, Commands.RAFT_QUERY_STATUS, new QueryStatusProcessor(this));

        if (serverConfig.getServicePort() > 0) {
            NioServerConfig serviceServerConfig = new NioServerConfig();
            serviceServerConfig.setPort(serverConfig.getServicePort());
            serviceServerConfig.setName("RaftServiceServer");
            serviceServerConfig.setBizThreads(0);
            // use multi io threads
            serviceNioServer = new NioServer(serviceServerConfig);
            addRaftGroupProcessor(serviceNioServer, Commands.RAFT_QUERY_LEADER, new QueryLeaderProcessor(this));
        } else {
            serviceNioServer = null;
        }
    }

    private void addRaftGroupProcessor(NioServer nioServer, int command, RaftSequenceProcessor<?> processor) {
        nioServer.register(command, processor);
        raftSequenceProcessors.add(processor);
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

        GroupComponents gc = new GroupComponents();

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
        raftStatus.setTailCache(new TailCache(serverConfig, raftStatus));
        raftStatus.setNodeIdOfMembers(nodeIdOfMembers);
        raftStatus.setNodeIdOfObservers(nodeIdOfObservers);
        raftStatus.setGroupId(rgc.getGroupId());

        RaftGroupConfigEx rgcEx = createGroupConfigEx(rgc, raftStatus, fiberGroup);

        gc.setServerConfig(serverConfig);
        gc.setGroupConfig(rgcEx);
        gc.setRaftStatus(raftStatus);
        gc.setFiberGroup(fiberGroup);

        StateMachine stateMachine = raftFactory.createStateMachine(rgcEx);
        rgcEx.setCodecFactory(stateMachine);
        StatusManager statusManager = new StatusManager(rgcEx);
        RaftLog raftLog = raftFactory.createRaftLog(rgcEx, statusManager);

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
        gc.setSnapshotManager(raftFactory.createSnapshotManager(rgcEx));
        gc.setStatusManager(statusManager);

        applyManager.postInit();
        commitManager.postInit();
        replicateManager.postInit();
        memberManager.postInit();
        linearTaskRunner.postInit();
        voteManager.postInit();

        return new RaftGroupImpl(gc);
    }

    private RaftGroupConfigEx createGroupConfigEx(RaftGroupConfig rgc, RaftStatusImpl raftStatus,
                                                  FiberGroup fiberGroup) {
        RaftGroupConfigEx rgcEx = new RaftGroupConfigEx(rgc.getGroupId(), rgc.getNodeIdOfMembers(),
                rgc.getNodeIdOfObservers());
        rgcEx.setDataDir(rgc.getDataDir());
        rgcEx.setStatusFile(rgc.getStatusFile());
        rgcEx.setIoRetryInterval(rgc.getIoRetryInterval());

        rgcEx.setTs(raftStatus.getTs());
        rgcEx.setHeapPool(createHeapPoolFactory(fiberGroup));
        rgcEx.setDirectPool(serverConfig.getPoolFactory().apply(raftStatus.getTs(), true));
        rgcEx.setRaftStatus(raftStatus);
        rgcEx.setIoExecutor(raftFactory.createIoExecutor());
        rgcEx.setFiberGroup(fiberGroup);

        return rgcEx;
    }

    private RefBufferFactory createHeapPoolFactory(FiberGroup fiberGroup) {
        ExecutorService executorService = fiberGroup.getExecutor();
        Dispatcher dispatcher = fiberGroup.getDispatcher();

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
                gc.getMemberManager().init(nodeManager.getAllNodesEx());
                CompletableFuture<Void> f = raftFactory.startFiberGroup(gc.getFiberGroup());
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

    private void initRaftGroup(RaftGroupImpl g) {
        GroupComponents gc = g.getGroupComponents();
        InitFiberFrame initFiberFrame = new InitFiberFrame(gc, raftSequenceProcessors);
        Fiber initFiber = new Fiber("init-raft-group-" + g.getGroupId(),
                gc.getFiberGroup(), initFiberFrame);
        gc.getFiberGroup().fireFiber(initFiber);
        initFiberFrame.getPrepareFuture().whenComplete((v, ex) -> {
            if (ex != null) {
                gc.getRaftStatus().getInitFuture().completeExceptionally(ex);
            } else {
                gc.getRaftStatus().getInitFuture().complete(null);
            }
        });
    }

    private void startServers() {
        try {
            nodeManager.start();
            nodeManager.readyFuture().whenComplete((v, ex) -> {
                if (ex != null) {
                    readyFuture.completeExceptionally(ex);
                } else if (status == STATUS_STARTING) {
                    startGroups();
                } else {
                    readyFuture.completeExceptionally(new IllegalStateException("server is not starting"));
                }
            });
        } catch (Exception e) {
            log.error("start node manager failed", e);
            throw new RaftException(e);
        }
    }

    private void startGroups() {
        try {
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
            raftGroups.forEach((groupId, g) -> {
                startRaftGroup(g);
                futures.add(g.getGroupComponents().getRaftStatus().getStartFuture());
            });

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((v, ex) -> {
                if (ex != null) {
                    readyFuture.completeExceptionally(ex);
                } else if (status == STATUS_STARTING) {
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
            log.error("start raft groups failed", e);
            throw new RaftException(e);
        }
    }

    private void startRaftGroup(RaftGroupImpl g) {
        GroupComponents gc = g.getGroupComponents();
        gc.getFiberGroup().fireFiber(gc.getMemberManager().createRaftPingFiber());
        gc.getMemberManager().getStartReadyFuture().whenComplete((v, ex) -> {
            if (ex != null) {
                gc.getRaftStatus().getStartFuture().completeExceptionally(ex);
            } else {
                gc.getRaftStatus().getStartFuture().complete(null);
            }
        });
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
                g.setRequestShutdown(true);
                raftFactory.requestGroupShutdown(g.getFiberGroup());
                futures.add(g.getFiberGroup().getShutdownFuture());
            });
            nodeManager.stop(timeout);

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
                    replicateNioClient.stop(timeout);
                }
            }
        } catch (RuntimeException | Error e) {
            log.error("stop raft server failed", e);
            throw e;
        }
    }

    private void checkStatus() {
        if (status != STATUS_RUNNING) {
            throw new RaftException("raft server is not running");
        }
    }

    private <T> T doChange(long acquireLockTimeoutMillis, Supplier<T> supplier) {
        checkStatus();
        boolean lock = false;
        try {
            lock = changeLock.tryLock(acquireLockTimeoutMillis, TimeUnit.MILLISECONDS);
            if (!lock) {
                throw new RaftException("failed to acquire change lock in " + acquireLockTimeoutMillis + "ms");
            }
            return supplier.get();
        } catch (InterruptedException e) {
            DtUtil.restoreInterruptStatus();
            throw new RaftException(e);
        } finally {
            if (lock) {
                changeLock.unlock();
            }
        }
    }

    /**
     * ADMIN API. This method is idempotent and may block. When complete the new node is connected.
     * If the node is already in node list and connected, the future complete normally immediately.
     */
    @SuppressWarnings("unused")
    public void addNode(RaftNode node, long acquireLockTimeoutMillis) {
        doChange(acquireLockTimeoutMillis, () -> {
            try {
                nodeManager.addNode(node).get();
                return null;
            } catch (InterruptedException e) {
                DtUtil.restoreInterruptStatus();
                throw new RaftException(e);
            } catch (Exception e) {
                throw new RaftException(e);
            }
        });
    }

    /**
     * ADMIN API. This method is idempotent and may block. If the node is node in node list, complete normally immediately.
     * If the reference count of the node is not 0, the future complete exceptionally.
     */
    @SuppressWarnings("unused")
    public void removeNode(int nodeId, long acquireLockTimeoutMillis) {
        doChange(acquireLockTimeoutMillis, () -> {
            try {
                nodeManager.removeNode(nodeId).get();
                return null;
            } catch (InterruptedException e) {
                DtUtil.restoreInterruptStatus();
                throw new RaftException(e);
            } catch (Exception e) {
                throw new RaftException(e);
            }
        });
    }

    /**
     * ADMIN API. This method is NOT idempotent.
     * <p>
     * This method may block (before return future).
     * After get the CompletableFuture, user should wait on the future to ensure raft group is initialized.
     */
    @SuppressWarnings("unused")
    public CompletableFuture<Void> addGroup(RaftGroupConfig groupConfig, long acquireLockTimeoutMillis) {
        return doChange(acquireLockTimeoutMillis, () -> {
            try {
                if (raftGroups.get(groupConfig.getGroupId()) != null) {
                    return CompletableFuture.failedFuture(new RaftException(
                            "group already exist: " + groupConfig.getGroupId()));
                }
                CompletableFuture<RaftGroupImpl> f = new CompletableFuture<>();
                RaftUtil.SCHEDULED_SERVICE.execute(() -> {
                    try {
                        RaftGroupImpl g = createRaftGroup(serverConfig, nodeManager.getAllNodeIds(), groupConfig);
                        g.getGroupComponents().getMemberManager().init(nodeManager.getAllNodesEx());
                        f.complete(g);
                    } catch (Exception e) {
                        f.completeExceptionally(e);
                    }
                });

                RaftGroupImpl g = f.get();
                GroupComponents gc = g.getGroupComponents();

                raftFactory.startFiberGroup(gc.getFiberGroup()).get();

                raftGroups.put(groupConfig.getGroupId(), g);
                initRaftGroup(g);
                RaftStatusImpl raftStatus = g.getGroupComponents().getRaftStatus();
                return raftStatus.getInitFuture().thenCompose(v -> {
                    startRaftGroup(g);
                    return raftStatus.getStartFuture();
                });
            } catch (InterruptedException e) {
                DtUtil.restoreInterruptStatus();
                return CompletableFuture.failedFuture(e);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        });
    }

    /**
     * ADMIN API. This method is idempotent.
     */
    @SuppressWarnings("unused")
    public CompletableFuture<Void> removeGroup(int groupId, long acquireLockTimeoutMillis) {
        return doChange(acquireLockTimeoutMillis, () -> {
            try {
                RaftGroupImpl g = raftGroups.get(groupId);
                if (g == null) {
                    log.warn("removeGroup failed: group not exist, groupId={}", groupId);
                    return CompletableFuture.failedFuture(new RaftException("group not exist: " + groupId));
                }
                if (g.isRequestShutdown()) {
                    return g.getFiberGroup().getShutdownFuture();
                }
                g.setRequestShutdown(true);
                raftFactory.requestGroupShutdown(g.getFiberGroup());
                return g.getFiberGroup().getShutdownFuture().thenRun(() -> raftGroups.remove(groupId));
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        });
    }

    public RaftGroup getRaftGroup(int groupId) {
        return raftGroups.get(groupId);
    }

    public NioServer getServiceNioServer() {
        return serviceNioServer;
    }
}
