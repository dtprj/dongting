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
package com.github.dtprj.dongting.raft;

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.NetException;
import com.github.dtprj.dongting.net.NetTimeoutException;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.PbIntWritePacket;
import com.github.dtprj.dongting.net.Peer;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.WritePacket;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * @author huangli
 */
public class RaftClient extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftClient.class);
    private final RaftClientConfig config;
    protected final NioClient nioClient;
    // key is nodeId
    private final ConcurrentHashMap<Integer, RaftNode> allNodes = new ConcurrentHashMap<>();
    // key is groupId
    private final ConcurrentHashMap<Integer, GroupInfo> groups = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    public RaftClient() {
        this(new RaftClientConfig(), new NioClientConfig("RaftClient"));
    }

    public RaftClient(RaftClientConfig raftClientConfig, NioClientConfig nioClientConfig) {
        this.config = raftClientConfig;
        this.nioClient = new NioClient(nioClientConfig);
    }

    private void checkStatus() {
        if (status != AbstractLifeCircle.STATUS_RUNNING) {
            throw new IllegalStateException("RaftClient is not running");
        }
    }

    public void clientAddNode(String servers) {
        List<RaftNode> list = RaftNode.parseServers(servers);
        clientAddNode(list);
    }

    public void clientAddNode(List<RaftNode> nodes) {
        requireNonNull(nodes);
        nodes.forEach(n -> DtUtil.checkPositive(n.nodeId, "nodeId"));
        if (nodes.stream().map(n -> n.nodeId).distinct().count() != nodes.size()) {
            throw new IllegalArgumentException("duplicated node id");
        }
        if (nodes.stream().map(n -> n.hostPort).distinct().count() != nodes.size()) {
            throw new IllegalArgumentException("duplicated host port");
        }
        checkStatus();
        lock.lock();
        try {
            HashSet<HostPort> existingHostPorts = new HashSet<>(allNodes.size() << 1);
            for (Map.Entry<Integer, RaftNode> e : allNodes.entrySet()) {
                existingHostPorts.add(e.getValue().hostPort);
            }
            for (RaftNode newNode : nodes) {
                if (allNodes.get(newNode.nodeId) != null) {
                    throw new RaftException("node " + newNode.nodeId + " already exists");
                }
                if (existingHostPorts.contains(newNode.hostPort)) {
                    throw new RaftException("node " + newNode.hostPort + " already exists");
                }
            }
            ArrayList<CompletableFuture<RaftNode>> futures = new ArrayList<>();
            for (RaftNode n : nodes) {
                // this operation should finish quickly
                CompletableFuture<Peer> f = nioClient.addPeer(n.hostPort);
                futures.add(f.thenApply(peer -> new RaftNode(n.nodeId, n.hostPort, peer)));
            }
            boolean success = false;
            try {
                DtTime timeout = new DtTime(10, TimeUnit.SECONDS);
                for (CompletableFuture<RaftNode> f : futures) {
                    long rest = timeout.rest(TimeUnit.MILLISECONDS);
                    if (rest > 0) {
                        RaftNode n = f.get(rest, TimeUnit.MILLISECONDS);
                        allNodes.put(n.nodeId, n);
                    } else {
                        throw new TimeoutException();
                    }
                }
                success = true;
            } catch (InterruptedException e) {
                DtUtil.restoreInterruptStatus();
                throw new NetException(e);
            } catch (Exception e) {
                throw new NetException(e);
            } finally {
                if (!success) {
                    // all success or all fail
                    for (RaftNode n : nodes) {
                        nioClient.removePeer(n.hostPort);
                        allNodes.remove(n.nodeId);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void clientRemoveNode(int... nodeIds) {
        checkStatus();
        lock.lock();
        try {
            for (int id : nodeIds) {
                RaftNode n = allNodes.get(id);
                if (n.useCount > 0) {
                    throw new RaftException("node " + id + " is in use: useCount=" + n.useCount);
                }
            }
            for (int id : nodeIds) {
                RaftNode n = allNodes.remove(id);
                if (n != null) {
                    nioClient.removePeer(n.hostPort);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void clientAddOrUpdateGroup(int groupId, int[] serverIds) throws NetException {
        requireNonNull(serverIds);
        DtUtil.checkNotNegative(groupId, "groupId");
        int len = serverIds.length;
        if (len == 0) {
            throw new IllegalArgumentException("servers is empty");
        }
        for (int i = 0; i < len; i++) {
            DtUtil.checkPositive(serverIds[i], "serverIds");
            for (int j = 0; j < len; j++) {
                if (i != j && serverIds[i] == serverIds[j]) {
                    throw new IllegalArgumentException("duplicated server id: " + serverIds[i]);
                }
            }
        }
        checkStatus();
        log.info("add or update group {}, serverIds={}", groupId, Arrays.toString(serverIds));
        lock.lock();
        try {
            addOrUpdateGroupInLock(groupId, serverIds);
        } finally {
            lock.unlock();
        }
    }

    private void addOrUpdateGroupInLock(int groupId, int[] serverIds) throws NetException {
        for (int id : serverIds) {
            if (allNodes.get(id) == null) {
                throw new RaftException("node " + id + " not exist");
            }
        }
        GroupInfo oldGroupInfo = groups.get(groupId);
        if (!isMemberChanged(serverIds, oldGroupInfo)) {
            return;
        }
        List<RaftNode> managedServers = new ArrayList<>();
        RaftNode leader = null;
        for (int nodeId : serverIds) {
            RaftNode n = allNodes.get(nodeId);
            managedServers.add(n);
            n.useCount++;
            if (oldGroupInfo != null) {
                RaftNode oldLeader = oldGroupInfo.leader;
                if (oldLeader != null && oldLeader == n) {
                    // old leader in the new servers list
                    leader = oldLeader;
                }
            }
        }

        if (oldGroupInfo != null) {
            for (RaftNode n : oldGroupInfo.servers) {
                n.useCount--;
            }
        }

        for (RaftNode n : managedServers) {
            if (n.peer.status == PeerStatus.not_connect) {
                nioClient.connect(n.peer);
            }
        }

        Collections.shuffle(managedServers);
        GroupInfo gi;
        int epoch = oldGroupInfo == null ? 0 : oldGroupInfo.serversEpoch + 1;
        boolean oldLeaderOK = leader != null && leader.peer.status == PeerStatus.connected
                && oldGroupInfo.lastLeaderFailTime == null;
        boolean findInProgress = oldGroupInfo != null && oldGroupInfo.leaderFuture != null
                && !oldGroupInfo.leaderFuture.isDone();
        if (oldLeaderOK && !findInProgress) {
            gi = new GroupInfo(groupId, unmodifiableList(managedServers), epoch, leader, false);
            log.info("use old leader for group {}, leader={}", groupId, leader.nodeId);
        } else {
            gi = new GroupInfo(groupId, unmodifiableList(managedServers), epoch, leader, true);
            log.info("find leader for group {}", groupId);
            findLeader(gi, gi.servers.iterator());
            finishOldGroupFutureIfNecessary(gi, oldGroupInfo);
        }
        groups.put(groupId, gi);
    }

    private GroupInfo createAndPutGroupInfo(GroupInfo old, RaftNode leader, boolean createFuture) {
        GroupInfo gi = new GroupInfo(old.groupId, old.servers, old.serversEpoch + 1, leader, createFuture);
        finishOldGroupFutureIfNecessary(gi, old);
        groups.put(gi.groupId, gi);
        return gi;
    }

    private void finishOldGroupFutureIfNecessary(GroupInfo newGroupInfo, GroupInfo oldGroupInfo) {
        // use new leader future to complete the old one
        if (oldGroupInfo != null && oldGroupInfo.leaderFuture != null && !oldGroupInfo.leaderFuture.isDone()) {
            if (newGroupInfo.leaderFuture != null) {
                newGroupInfo.leaderFuture.whenComplete((result, ex) -> {
                    if (ex != null) {
                        oldGroupInfo.leaderFuture.completeExceptionally(ex);
                    } else {
                        oldGroupInfo.leaderFuture.complete(result);
                    }
                });
            } else if (newGroupInfo.leader != null) {
                oldGroupInfo.leaderFuture.complete(newGroupInfo);
            } else {
                oldGroupInfo.leaderFuture.completeExceptionally(new RaftException(
                        "can't found leader for group " + newGroupInfo.groupId));
            }
        }
    }

    private static boolean isMemberChanged(int[] serverIds, GroupInfo oldGroupInfo) {
        if (oldGroupInfo == null || oldGroupInfo.servers.size() != serverIds.length) {
            return true;
        } else {
            HashSet<Integer> oldServerIds = new HashSet<>();
            for (RaftNode n : oldGroupInfo.servers) {
                oldServerIds.add(n.nodeId);
            }
            HashSet<Integer> newServerIds = new HashSet<>();
            for (int id : serverIds) {
                newServerIds.add(id);
            }
            return !oldServerIds.equals(newServerIds);
        }
    }

    public void clientRemoveGroup(int groupId) throws NetException {
        checkStatus();
        lock.lock();
        try {
            GroupInfo oldGroupInfo = groups.remove(groupId);
            if (oldGroupInfo != null) {
                if (oldGroupInfo.leaderFuture != null) {
                    oldGroupInfo.leaderFuture.completeExceptionally(new RaftException("group removed " + groupId));
                }
                for (RaftNode n : oldGroupInfo.servers) {
                    n.useCount--;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    protected CompletableFuture<QueryStatusResp> queryRaftServerStatus(int nodeId, int groupId) {
        checkStatus();
        RaftNode n = getNode(nodeId);
        if (n == null) {
            return DtUtil.failedFuture(new RaftException("node not found: " + nodeId));
        }
        PbIntWritePacket req = new PbIntWritePacket(Commands.RAFT_QUERY_STATUS, groupId);
        CompletableFuture<QueryStatusResp> f = new CompletableFuture<>();
        RpcCallback<QueryStatusResp> callback = RpcCallback.fromUnwrapFuture(f);
        nioClient.sendRequest(n.peer, req, QueryStatusResp.DECODER, createDefaultTimeout(), callback);
        return f;
    }

    private static class SyncFutureCallback<T> implements RpcCallback<T> {

        private final CompletableFuture<ReadPacket<T>> future = new CompletableFuture<>();

        @Override
        public void call(ReadPacket<T> result, Throwable ex) {
            if (ex != null) {
                future.completeExceptionally(ex);
            } else {
                future.complete(result);
            }
        }
    }

    /**
     * Sync send request to raft leader of the group.
     * If current leader is unknown try to find leader first.
     * If receive NOT_RAFT_LEADER and with new leader info in extra, redirect the request to new leader.
     */
    public <T> ReadPacket<T> sendRequest(Integer groupId, WritePacket request, DecoderCallbackCreator<T> decoder, DtTime timeout) {
        SyncFutureCallback<T> c = new SyncFutureCallback<>();
        sendRequest(groupId, request, decoder, timeout, c);
        return waitFuture(c.future, timeout);
    }

    private <T> T waitFuture(CompletableFuture<T> f, DtTime timeout) {
        try {
            return f.get(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            DtUtil.restoreInterruptStatus();
            throw new NetException("interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = DtUtil.rootCause(e);
            if (cause instanceof InterruptedException) {
                DtUtil.restoreInterruptStatus();
            }
            throw new NetException(e);
        } catch (TimeoutException e) {
            throw new NetTimeoutException("timeout: " + timeout.getTimeout(TimeUnit.MILLISECONDS) + "ms", e);
        }
    }

    /**
     * Async send request to raft leader of the group.
     * If current leader is unknown try to find leader first.
     * If receive NOT_RAFT_LEADER and with new leader info in extra, redirect the request to new leader.
     */
    public <T> void sendRequest(Integer groupId, WritePacket request, DecoderCallbackCreator<T> decoder,
                                DtTime timeout, RpcCallback<T> callback) {
        checkStatus();
        GroupInfo groupInfo = groups.get(groupId);
        if (groupInfo == null) {
            invokeOriginCallback(callback, null, new NoSuchGroupException(groupId));
            return;
        }
        boolean getPermit = false;
        try {
            getPermit = nioClient.acquirePermit(request, timeout);
            if (groupInfo.leader != null && groupInfo.leader.peer.status == PeerStatus.connected
                    && (groupInfo.lastLeaderFailTime == null ||
                    System.nanoTime() - groupInfo.lastLeaderFailTime.createTimeNanos < 1_000_000_000)) {
                // one raft group has only one leader, if leader rpc fails, don't trigger findLeader at once,
                // the request still send to the current leader
                send(groupInfo, request, decoder, timeout, callback, 0, getPermit);
            } else {
                sendAfterUpdateLeader(groupId, request, decoder, 0, timeout, callback, groupInfo, getPermit);
            }
        } catch (Throwable e) {
            handleSendEx(request, callback, e, getPermit);
        }
    }

    private <T> void sendAfterUpdateLeader(Integer groupId, WritePacket request, DecoderCallbackCreator<T> decoder,
                                           int retry, DtTime timeout, RpcCallback<T> callback, GroupInfo groupInfo,
                                           boolean getPermit) {
        CompletableFuture<GroupInfo> leaderFuture;
        if (groupInfo.leaderFuture == null) {
            leaderFuture = updateLeaderInfo(groupId, false);
        } else {
            leaderFuture = groupInfo.leaderFuture;
        }
        leaderFuture.whenComplete((gi, ex) -> {
            if (ex != null) {
                if (getPermit) {
                    nioClient.releasePermit(request);
                }
                invokeOriginCallback(callback, null, ex);
            } else if (timeout.isTimeout()) {
                if (getPermit) {
                    nioClient.releasePermit(request);
                }
                invokeOriginCallback(callback, null, new RaftTimeoutException(
                        "timeout after find leader for group " + groupId));
            } else {
                try {
                    send(gi, request, decoder, timeout, callback, retry, getPermit);
                } catch (Throwable e) {
                    handleSendEx(request, callback, e, getPermit);
                }
            }
        });
    }

    private <T> void handleSendEx(WritePacket request, RpcCallback<T> callback, Throwable e, boolean getPermit) {
        Throwable root = DtUtil.rootCause(e);
        if (e instanceof InterruptedException || root instanceof InterruptedException) {
            DtUtil.restoreInterruptStatus();
        }
        if (getPermit) {
            nioClient.releasePermit(request);
        }
        invokeOriginCallback(callback, null, e);
    }

    private <T> void send(GroupInfo groupInfo, WritePacket request, DecoderCallbackCreator<T> decoder,
                          DtTime timeout, RpcCallback<T> c, int retry, boolean getPermit) {
        RpcCallback<T> newCallback = (result, ex) -> wrapCallback(groupInfo, request, decoder, timeout, c,
                retry, getPermit, result, ex);
        nioClient.sendRequest(groupInfo.leader.peer, request, decoder, timeout, newCallback);
    }

    private <T> void wrapCallback(GroupInfo groupInfo, WritePacket request, DecoderCallbackCreator<T> decoder,
                                  DtTime timeout, RpcCallback<T> c, int retry, boolean getPermit,
                                  ReadPacket<T> result, Throwable ex) {
        boolean shouldRelease = getPermit;
        boolean shouldInvokeCallback = true;
        try {
            if (ex instanceof NetCodeException) {
                NetCodeException ncEx = (NetCodeException) ex;
                switch (ncEx.getCode()) {
                    case CmdCodes.NOT_RAFT_LEADER: {
                        GroupInfo newGroupInfo = updateLeaderFromExtra(ncEx.getExtra(), groupInfo);
                        if (request.canRetry() && retry == 0) {
                            if (status == STATUS_RUNNING && newGroupInfo != null && newGroupInfo.leader != null
                                    && !timeout.isTimeout()) {
                                log.info("leader changed, update leader from node {} to {}, request will auto retry",
                                        groupInfo.leader.nodeId, newGroupInfo.leader.nodeId);
                                try {
                                    request.prepareRetry();
                                    send(newGroupInfo, request, decoder, timeout, c, 1, getPermit);
                                    // not release permit, it's released in the callback of send()
                                } catch (Throwable retryEx) {
                                    // release in handleSendEx
                                    handleSendEx(request, c, retryEx, getPermit);
                                }
                                shouldRelease = false;
                                shouldInvokeCallback = false;
                                return;
                            }
                        }
                        break;
                    }
                    case CmdCodes.NOT_INIT:
                    case CmdCodes.STOPPING:
                    case CmdCodes.RAFT_GROUP_NOT_INIT:
                    case CmdCodes.RAFT_GROUP_NOT_FOUND:
                    case CmdCodes.RAFT_GROUP_STOPPED: {
                        updateLeaderFailTime(groupInfo);
                        if (request.canRetry() && retry == 0 && status == STATUS_RUNNING) {
                            try {
                                request.prepareRetry();
                                // the groupInfo is not latest, it's ok here
                                sendAfterUpdateLeader(groupInfo.groupId, request, decoder, 1, timeout,
                                        c, groupInfo, getPermit);
                                // not release permit, it's released in the callback of send()
                            } catch (Throwable retryEx) {
                                // release in handleSendEx
                                handleSendEx(request, c, retryEx, getPermit);
                            }
                            shouldRelease = false;
                            shouldInvokeCallback = false;
                            return;
                        }
                        break;
                    }
                    case CmdCodes.SYS_ERROR:
                    case CmdCodes.FLOW_CONTROL:
                        updateLeaderFailTime(groupInfo);
                        break;
                    case CmdCodes.COMMAND_NOT_SUPPORT:
                    case CmdCodes.CLIENT_ERROR:
                        break;
                    default:
                        log.error("unknown error code: {}", ncEx.getCode());
                        updateLeaderFailTime(groupInfo);
                }
            } else if (ex != null) {
                updateLeaderFailTime(groupInfo);
            }
        } catch (Exception wrapperCallbackEx) {
            log.error("raft client callback error", wrapperCallbackEx);
            //noinspection ConstantValue
            if (ex != null && ex != wrapperCallbackEx) {
                ex.addSuppressed(wrapperCallbackEx);
            }
        } finally {
            if (shouldRelease) {
                nioClient.releasePermit(request);
            }
            if (shouldInvokeCallback) {
                invokeOriginCallback(c, result, ex);
            }
        }
    }

    private <T> void invokeOriginCallback(RpcCallback<T> c, ReadPacket<T> result, Throwable ex) {
        if (c != null) {
            boolean callInBizExecutor = config.useBizExecutor && nioClient.getBizExecutor() != null
                    && (!(c instanceof SyncFutureCallback));
            if (callInBizExecutor) {
                try {
                    nioClient.getBizExecutor().submit(() -> {
                        try {
                            c.call(result, ex);
                        } catch (Throwable callbackEx) {
                            log.error("RaftClient callback error", callbackEx);
                        }
                    });
                } catch (RejectedExecutionException rejectEx) {
                    log.error("callback task submit rejected");
                    try {
                        if (ex != null && ex != rejectEx) {
                            ex.addSuppressed(rejectEx);
                        }
                        c.call(result, ex);
                    } catch (Throwable callbackEx) {
                        log.error("RaftClient callback error", callbackEx);
                    }
                }
            } else {
                try {
                    c.call(result, ex);
                } catch (Throwable callbackEx) {
                    log.error("RaftClient callback error", callbackEx);
                }
            }
        }
    }

    private void updateLeaderFailTime(GroupInfo oldGroupInfo) {
        GroupInfo currentGroupInfo = groups.get(oldGroupInfo.groupId);
        if (currentGroupInfo != oldGroupInfo || currentGroupInfo.lastLeaderFailTime != null) {
            return;
        }
        lock.lock();
        try {
            currentGroupInfo = groups.get(oldGroupInfo.groupId);
            if (currentGroupInfo != oldGroupInfo || currentGroupInfo.lastLeaderFailTime != null) {
                return;
            }
            GroupInfo newGroupInfo = GroupInfo.createByLastLeaderFailTime(oldGroupInfo, new DtTime());
            groups.put(oldGroupInfo.groupId, newGroupInfo);
        } finally {
            lock.unlock();
        }
    }

    private GroupInfo updateLeaderFromExtra(byte[] extra, GroupInfo lastReqGroupInfo) {
        if (extra == null) {
            log.warn("receive NOT_RAFT_LEADER, but no new leader info, groupId={}", lastReqGroupInfo.groupId);
            return null;
        }
        int suggestLeaderId;
        try {
            suggestLeaderId = Integer.parseInt(new String(extra, StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            log.error("receive NOT_RAFT_LEADER with invalid new leader, groupId={}", lastReqGroupInfo.groupId);
            return null;
        }
        lock.lock();
        try {
            GroupInfo currentGroupInfo = groups.get(lastReqGroupInfo.groupId);
            if (currentGroupInfo == null) {
                log.error("group {} is removed", lastReqGroupInfo.groupId);
                return null;
            }
            if (currentGroupInfo.serversEpoch != lastReqGroupInfo.serversEpoch) {
                if (currentGroupInfo.leader != null && currentGroupInfo.leader.nodeId == suggestLeaderId) {
                    return currentGroupInfo;
                } else {
                    // group info changed, drop the result
                    log.warn("groupInfo changed, groupId={}", lastReqGroupInfo.groupId);
                    return null;
                }
            }
            RaftNode leader = parseLeader(lastReqGroupInfo, suggestLeaderId);
            if (leader != null) {
                return createAndPutGroupInfo(lastReqGroupInfo, leader, false);
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<RaftNode> fetchLeader(int groupId) {
        checkStatus();
        return updateLeaderInfo(groupId, true).thenApply(gi -> gi.leader);
    }

    protected CompletableFuture<GroupInfo> updateLeaderInfo(Integer groupId, boolean force) {
        lock.lock();
        try {
            GroupInfo gi = groups.get(groupId);
            if (gi == null) {
                return DtUtil.failedFuture(new NoSuchGroupException(groupId));
            }
            if (gi.leader != null && gi.leader.peer.status == PeerStatus.connected
                    && gi.lastLeaderFailTime == null && !force) {
                return CompletableFuture.completedFuture(gi);
            }
            if (gi.leaderFuture != null && !force) {
                return gi.leaderFuture;
            }
            if (gi.lastFindFailTime != null && !force &&
                    System.nanoTime() - gi.lastFindFailTime.createTimeNanos < 1_000_000_000) {
                return DtUtil.failedFuture(new RaftException("find leader fail recently"));
            }
            GroupInfo newGroupInfo = createAndPutGroupInfo(gi, gi.leader, true);
            ArrayList<RaftNode> servers = new ArrayList<>(newGroupInfo.servers);
            Collections.shuffle(servers);
            Iterator<RaftNode> it = servers.iterator();
            log.info("try find leader for group {}", groupId);
            findLeader(newGroupInfo, it);
            return newGroupInfo.leaderFuture;
        } finally {
            lock.unlock();
        }
    }

    private void findLeader(GroupInfo gi, Iterator<RaftNode> it) {
        if (!it.hasNext()) {
            //noinspection DataFlowIssue
            gi.leaderFuture.completeExceptionally(new RaftException("can't find leader for group " + gi.groupId));

            log.error("can't find leader for group {}", gi.groupId);
            // set new group info, to trigger next find
            GroupInfo newGroupInfo = GroupInfo.createByLastFindFailTime(gi, new DtTime());
            groups.put(gi.groupId, newGroupInfo);
            return;
        }
        RaftNode node = it.next();
        try {
            CompletableFuture<QueryStatusResp> s = queryRaftServerStatus(node.nodeId, gi.groupId);
            s.whenComplete((resp, ex) -> processLeaderQueryResult(gi, it, resp, ex, node));
        } catch (Exception e) {
            processLeaderQueryResult(gi, it, null, e, node);
        }
    }

    private void processLeaderQueryResult(GroupInfo gi, Iterator<RaftNode> it, QueryStatusResp status,
                                          Throwable ex, RaftNode node) {
        lock.lock();
        try {
            GroupInfo currentGroupInfo = groups.get(gi.groupId);
            if (currentGroupInfo == null) {
                requireNonNull(gi.leaderFuture).completeExceptionally(new RaftException("group id removed: " + gi.groupId));
                return;
            }
            if (currentGroupInfo.serversEpoch != gi.serversEpoch) {
                // group member changed, stop current find process, and wait new find action complete the future
                return;
            }
            if (ex != null) {
                log.warn("query leader from {} fail: {}", node, ex.toString());
                findLeader(gi, it);
            } else {
                if (status == null) {
                    log.error("query leader from {} fail, result is null", node);
                    findLeader(gi, it);
                } else if (status.leaderId < 0) {
                    log.error("query leader from {} fail, leader id illegal: {}", node, status.leaderId);
                    findLeader(gi, it);
                } else if (status.leaderId == 0) {
                    log.error("node {} has no leader now", node.nodeId);
                    findLeader(gi, it);
                } else {
                    RaftNode leader = parseLeader(gi, status.leaderId);
                    if (leader != null) {
                        log.debug("find leader for group {}: {}", gi.groupId, leader);
                        if (leader.peer.status == PeerStatus.connected) {
                            log.info("group {} connected to leader: {}", gi.groupId, leader);
                            createAndPutGroupInfo(gi, leader, false);
                        } else {
                            try {
                                nioClient.connect(leader.peer)
                                        .whenComplete((v, e) -> connectToLeaderCallback(gi, leader, e));
                            } catch (Exception e) {
                                log.error("connect to leader error", e);
                                connectToLeaderCallback(gi, leader, e);
                            }
                        }
                    } else {
                        findLeader(gi, it);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void connectToLeaderCallback(GroupInfo gi, RaftNode leader, Throwable e) {
        requireNonNull(gi.leaderFuture);
        lock.lock();
        try {
            GroupInfo currentGroupInfo = groups.get(gi.groupId);
            if (currentGroupInfo == null) {
                requireNonNull(gi.leaderFuture).completeExceptionally(new RaftException("group id removed: " + gi.groupId));
                return;
            }
            if (currentGroupInfo.serversEpoch != gi.serversEpoch) {
                // group member changed, stop current find process, and wait new find action complete the future
                return;
            }
            if (e != null) {
                log.warn("connect to leader {} fail: {}", leader, e.toString());
                RaftException re = new RaftException("connect to leader " + leader + " fail", e);
                gi.leaderFuture.completeExceptionally(re);
                createAndPutGroupInfo(gi, null, false);
            } else {
                log.info("group {} connected to leader: {}", gi.groupId, leader);
                createAndPutGroupInfo(gi, leader, false);
            }
        } finally {
            lock.unlock();
        }
    }

    private RaftNode parseLeader(GroupInfo groupInfo, int leaderId) {
        for (RaftNode ni : groupInfo.servers) {
            if (ni.nodeId == leaderId) {
                return ni;
            }
        }
        log.warn("leader {} not in group {}", leaderId, groupInfo.groupId);
        return null;
    }

    public GroupInfo getGroup(Integer groupId) {
        return groups.get(groupId);
    }

    public RaftNode getNode(Integer nodeId) {
        return allNodes.get(nodeId);
    }

    @Override
    protected void doStart() {
        nioClient.start();
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
        nioClient.stop(timeout);
    }

    public NioClient getNioClient() {
        return nioClient;
    }

    public DtTime createDefaultTimeout() {
        return new DtTime(config.rpcTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    public RaftClientConfig getConfig() {
        return config;
    }
}
