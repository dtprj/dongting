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
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.NioConfig;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.NioServerConfig;
import com.github.dtprj.dongting.raft.impl.ApplyManager;
import com.github.dtprj.dongting.raft.impl.CommitManager;
import com.github.dtprj.dongting.raft.impl.EventBus;
import com.github.dtprj.dongting.raft.impl.MemberManager;
import com.github.dtprj.dongting.raft.impl.NodeManager;
import com.github.dtprj.dongting.raft.impl.PendingStat;
import com.github.dtprj.dongting.raft.impl.Raft;
import com.github.dtprj.dongting.raft.impl.RaftExecutor;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftGroupThread;
import com.github.dtprj.dongting.raft.impl.RaftGroups;
import com.github.dtprj.dongting.raft.impl.RaftNodeEx;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.ReplicateManager;
import com.github.dtprj.dongting.raft.impl.VoteManager;
import com.github.dtprj.dongting.raft.rpc.AppendProcessor;
import com.github.dtprj.dongting.raft.rpc.InstallSnapshotProcessor;
import com.github.dtprj.dongting.raft.rpc.NodePingProcessor;
import com.github.dtprj.dongting.raft.rpc.RaftPingProcessor;
import com.github.dtprj.dongting.raft.rpc.TransferLeaderProcessor;
import com.github.dtprj.dongting.raft.rpc.VoteProcessor;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class RaftServer extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftServer.class);
    private final RaftFactory raftFactory;

    private final NioServer raftServer;
    private final NioClient raftClient;

    private final RaftGroups raftGroups = new RaftGroups();

    private final RaftServerConfig serverConfig;

    private final NodeManager nodeManager;

    private final AtomicBoolean change = new AtomicBoolean(false);

    private final PendingStat serverStat = new PendingStat();

    public RaftServer(Supplier<Boolean> cancelInitIndicator, RaftServerConfig serverConfig,
                      List<RaftGroupConfig> groupConfig, RaftFactory raftFactory) {
        Objects.requireNonNull(serverConfig);
        Objects.requireNonNull(groupConfig);
        Objects.requireNonNull(raftFactory);
        this.serverConfig = serverConfig;
        this.raftFactory = raftFactory;

        Objects.requireNonNull(serverConfig.getServers());
        DtUtil.checkPositive(serverConfig.getNodeId(), "id");
        DtUtil.checkPositive(serverConfig.getRaftPort(), "port");

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

        NioClientConfig nioClientConfig = new NioClientConfig();
        nioClientConfig.setName("RaftClient");
        setupNioConfig(nioClientConfig);
        raftClient = new NioClient(nioClientConfig);

        createRaftGroups(cancelInitIndicator, serverConfig, groupConfig, allNodeIds);
        nodeManager = new NodeManager(serverConfig, allRaftServers, raftClient, raftGroups);
        raftGroups.forEach((id, gc) -> gc.getEventBus().register(nodeManager));

        NioServerConfig nioServerConfig = new NioServerConfig();
        nioServerConfig.setPort(serverConfig.getRaftPort());
        nioServerConfig.setName("RaftServer");
        nioServerConfig.setBizThreads(0);
        nioServerConfig.setIoThreads(1);
        setupNioConfig(nioServerConfig);
        raftServer = new NioServer(nioServerConfig);
        raftServer.register(Commands.NODE_PING, new NodePingProcessor(serverConfig.getNodeId(), nodeManager.getUuid()));
        raftServer.register(Commands.RAFT_PING, new RaftPingProcessor(raftGroups));
        raftServer.register(Commands.RAFT_APPEND_ENTRIES, new AppendProcessor(raftGroups));
        raftServer.register(Commands.RAFT_REQUEST_VOTE, new VoteProcessor(raftGroups));
        raftServer.register(Commands.RAFT_INSTALL_SNAPSHOT, new InstallSnapshotProcessor(raftGroups));
        raftServer.register(Commands.RAFT_LEADER_TRANSFER, new TransferLeaderProcessor(raftGroups));
    }

    private void createRaftGroups(Supplier<Boolean> cancelInitIndicator, RaftServerConfig serverConfig,
                                  List<RaftGroupConfig> groupConfig, HashSet<Integer> allNodeIds) {
        for (RaftGroupConfig rgc : groupConfig) {
            RaftGroupImpl gc = createRaftGroup(cancelInitIndicator, serverConfig, allNodeIds, rgc);
            raftGroups.put(rgc.getGroupId(), gc);
        }
    }

    private RaftGroupImpl createRaftGroup(Supplier<Boolean> cancelInitIndicator, RaftServerConfig serverConfig,
                                          Set<Integer> allNodeIds, RaftGroupConfig rgc) {
        Objects.requireNonNull(rgc.getNodeIdOfMembers());

        EventBus eventBus = new EventBus();

        HashSet<Integer> nodeIdOfMembers = new HashSet<>();
        parseMemberIds(allNodeIds, nodeIdOfMembers, rgc.getNodeIdOfMembers(), rgc.getGroupId());
        if (nodeIdOfMembers.size() == 0 && serverConfig.isStaticConfig()) {
            throw new IllegalArgumentException("no member in group: " + rgc.getGroupId());
        }

        Set<Integer> nodeIdOfObservers;
        if (rgc.getNodeIdOfObservers() != null && rgc.getNodeIdOfObservers().trim().length() > 0) {
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

        RaftGroupThread raftGroupThread = new RaftGroupThread(cancelInitIndicator);
        RaftExecutor raftExecutor = new RaftExecutor(raftGroupThread);

        RaftStatusImpl raftStatus = new RaftStatusImpl();
        raftStatus.setRaftExecutor(raftExecutor);
        raftStatus.setNodeIdOfMembers(nodeIdOfMembers);
        raftStatus.setNodeIdOfObservers(nodeIdOfObservers);
        raftStatus.setGroupId(rgc.getGroupId());

        RaftGroupConfigEx rgcEx = createGroupConfigEx(rgc, raftStatus, raftExecutor, raftGroupThread);


        StateMachine stateMachine = raftFactory.createStateMachine(rgcEx);
        rgcEx.setCodecFactory(stateMachine);
        RaftLog raftLog = raftFactory.createRaftLog(rgcEx);

        MemberManager memberManager = new MemberManager(serverConfig, raftClient, raftExecutor,
                raftStatus, eventBus);
        ApplyManager applyManager = new ApplyManager(serverConfig.getNodeId(), raftLog, stateMachine, raftStatus, eventBus, rgcEx.getHeapPool());
        CommitManager commitManager = new CommitManager(raftStatus, applyManager);
        ReplicateManager replicateManager = new ReplicateManager(serverConfig, rgcEx, raftStatus, raftLog,
                stateMachine, raftClient, raftExecutor, commitManager);

        Raft raft = new Raft(raftStatus, raftLog, applyManager, commitManager, replicateManager);
        VoteManager voteManager = new VoteManager(serverConfig, rgc.getGroupId(), raftStatus, raftClient, raftExecutor, raft);

        eventBus.register(raft);
        eventBus.register(voteManager);

        RaftGroupImpl gc = new RaftGroupImpl();
        gc.setServerConfig(serverConfig);
        gc.setGroupConfig(rgc);
        gc.setRaftLog(raftLog);
        gc.setStateMachine(stateMachine);
        gc.setRaftGroupThread(raftGroupThread);
        gc.setRaftStatus(raftStatus);
        gc.setMemberManager(memberManager);
        gc.setVoteManager(voteManager);
        gc.setRaftExecutor(raftExecutor);
        gc.setApplyManager(applyManager);
        gc.setEventBus(eventBus);
        gc.setNodeManager(nodeManager);
        gc.setServerStat(serverStat);
        gc.setSnapshotManager(raftFactory.createSnapshotManager(rgcEx));
        return gc;
    }

    private RaftGroupConfigEx createGroupConfigEx(RaftGroupConfig rgc, RaftStatusImpl raftStatus,
                                                  RaftExecutor raftExecutor, RaftGroupThread raftGroupThread) {
        RaftGroupConfigEx rgcEx = new RaftGroupConfigEx(rgc.getGroupId(), rgc.getNodeIdOfMembers(),
                rgc.getNodeIdOfObservers());
        rgcEx.setDataDir(rgc.getDataDir());
        rgcEx.setStatusFile(rgc.getStatusFile());

        rgcEx.setTs(raftStatus.getTs());
        rgcEx.setHeapPool(createHeapPoolFactory(raftStatus.getTs(), raftExecutor, raftGroupThread));
        rgcEx.setDirectPool(serverConfig.getPoolFactory().apply(raftStatus.getTs(), true));
        rgcEx.setRaftExecutor(raftExecutor);
        rgcEx.setStopIndicator(raftStatus::isStop);
        rgcEx.setRaftStatus(raftStatus);

        return rgcEx;
    }

    private RefBufferFactory createHeapPoolFactory(Timestamp ts, RaftExecutor raftExecutor, RaftGroupThread raftGroupThread) {
        TwoLevelPool heapPool = (TwoLevelPool) serverConfig.getPoolFactory().apply(ts, false);
        TwoLevelPool releaseSafePool = heapPool.toReleaseInOtherThreadInstance(raftGroupThread, byteBuffer -> {
            if (byteBuffer != null) {
                raftExecutor.execute(() -> heapPool.release(byteBuffer));
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

    private void setupNioConfig(NioConfig nc) {
        nc.setFinishPendingImmediatelyWhenChannelClose(true);
        nc.setMaxOutRequests(0);
        nc.setMaxInRequests(0);
        nc.setMaxInBytes(0);
        nc.setMaxBodySize(Integer.MAX_VALUE);
        nc.setMaxFrameSize(Integer.MAX_VALUE);
    }

    @Override
    protected void doStart() {
        try {
            raftGroups.forEach((groupId, gc) -> gc.getRaftGroupThread().init(gc));

            raftServer.start();
            raftClient.start();
            raftClient.waitStart();

            nodeManager.start();
            nodeManager.waitReady(RaftUtil.getElectQuorum(nodeManager.getAllNodesEx().size()));
            log.info("nodeManager is ready");

            raftGroups.forEach((groupId, gc) -> {
                gc.getMemberManager().init(nodeManager.getAllNodesEx());
                gc.getRaftGroupThread().start();
            });

            raftGroups.forEach((groupId, gc) -> {
                gc.getRaftGroupThread().waitReady();
                log.info("raft group {} is ready", groupId);
            });
        } catch (RuntimeException | Error e) {
            log.error("start raft server failed", e);
            throw e;
        }
    }

    @Override
    protected void doStop() {
        try {
            raftGroups.forEach((groupId, gc) -> {
                RaftGroupThread raftGroupThread = gc.getRaftGroupThread();
                raftGroupThread.requestShutdown();
                raftGroupThread.interrupt();
            });
            raftServer.stop();
            raftClient.stop();
        } catch (RuntimeException | Error e) {
            log.error("stop raft server failed", e);
            throw e;
        }
    }

    private void checkStatus() {
        if (status != LifeStatus.running) {
            throw new RaftException("raft server is not running");
        }
    }

    private void doChange(Runnable runnable) {
        try {
            checkStatus();
            if (change.compareAndSet(false, true)) {
                try {
                    runnable.run();
                } catch (RaftException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RaftException(e);
                } finally {
                    change.set(false);
                }
            } else {
                throw new RaftException("raft server in changing");
            }
        } catch (RuntimeException | Error e) {
            log.error("doChange failed", e);
            throw e;
        }
    }

    /**
     * ADMIN API. This method is idempotent. When future complete the new node is connected.
     * If the node is already in node list and connected, the future complete normally immediately.
     */
    @SuppressWarnings("unused")
    public void addNode(RaftNode node, long timeoutMillis) {
        doChange(() -> {
            CompletableFuture<RaftNodeEx> f = nodeManager.addToNioClient(node);
            f = f.thenComposeAsync(nodeManager::addNode, RaftUtil.SCHEDULED_SERVICE);
            try {
                f.get(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new RaftException(e);
            }
        });
    }

    /**
     * ADMIN API. This method is idempotent. If the node is node in node list, the future complete normally immediately.
     * If the reference count of the node is not 0, the future complete exceptionally.
     */
    @SuppressWarnings("unused")
    public void removeNode(int nodeId) {
        doChange(() -> {
            try {
                CompletableFuture<Void> f = new CompletableFuture<>();
                RaftUtil.SCHEDULED_SERVICE.submit(() -> nodeManager.removeNode(nodeId, f));
                f.get();
            } catch (Exception e) {
                throw new RaftException(e);
            }
        });
    }

    /**
     * ADMIN API.
     */
    @SuppressWarnings("unused")
    public void addGroup(RaftGroupConfig groupConfig, Supplier<Boolean> cancelInitIndicator) {
        doChange(() -> {
            try {
                CompletableFuture<RaftGroupImpl> f = new CompletableFuture<>();
                RaftUtil.SCHEDULED_SERVICE.execute(() -> {
                    try {
                        RaftGroupImpl gc = createRaftGroup(cancelInitIndicator, serverConfig,
                                nodeManager.getAllNodeIds(), groupConfig);
                        gc.getMemberManager().init(nodeManager.getAllNodesEx());
                        f.complete(gc);
                    } catch (Exception e) {
                        f.completeExceptionally(e);
                    }
                });

                RaftGroupImpl gc = f.get(30, TimeUnit.SECONDS);
                gc.getRaftGroupThread().init(gc);

                gc.getRaftGroupThread().start();
                raftGroups.put(groupConfig.getGroupId(), gc);
            } catch (Exception e) {
                throw new RaftException(e);
            }
        });
    }

    /**
     * ADMIN API. This method is idempotent.
     */
    @SuppressWarnings("unused")
    public void removeGroup(int groupId, long timeoutMillis) {
        doChange(() -> {
            RaftGroupImpl gc = raftGroups.get(groupId);
            if (gc == null) {
                log.warn("removeGroup failed: group not exist, groupId={}", groupId);
                throw new RaftException("group not exist: " + groupId);
            }
            gc.getRaftGroupThread().requestShutdown();
            try {
                gc.getRaftGroupThread().join(timeoutMillis);
            } catch (InterruptedException e) {
                log.warn("removeGroup join interrupted, groupId={}", groupId);
                throw new RaftException("wait raft thread finish time out: " + timeoutMillis + "ms");
            } finally {
                raftGroups.remove(groupId);
            }
        });
    }

    @SuppressWarnings("unused")
    public RaftGroup getRaftGroup(int groupId) {
        return raftGroups.get(groupId);
    }

}
