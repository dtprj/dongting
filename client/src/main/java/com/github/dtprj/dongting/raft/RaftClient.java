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
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.NetException;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.PbIntWritePacket;
import com.github.dtprj.dongting.net.Peer;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.WritePacket;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public class RaftClient extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftClient.class);
    protected final NioClient nioClient;
    // key is nodeId
    private final ConcurrentHashMap<Integer, RaftNode> allNodes = new ConcurrentHashMap<>();
    // key is groupId
    private final ConcurrentHashMap<Integer, GroupInfo> groups = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    public long rpcTimeoutMillis = 5 * 1000L;

    public RaftClient(NioClientConfig nioClientConfig) {
        this.nioClient = new NioClient(nioClientConfig);
    }

    public void clientAddNode(String servers) {
        List<RaftNode> list = RaftNode.parseServers(servers);
        clientAddNode(list);
    }

    public void clientAddNode(List<RaftNode> nodes) {
        lock.lock();
        try {
            for (Map.Entry<Integer, RaftNode> e : allNodes.entrySet()) {
                for (RaftNode newNode : nodes) {
                    if (e.getKey() == newNode.nodeId) {
                        throw new RaftException("node " + e.getKey() + " already exists");
                    }
                    if (e.getValue().hostPort.equals(newNode.hostPort)) {
                        throw new RaftException("node " + e.getValue().hostPort + " already exists");
                    }
                }
            }
            ArrayList<CompletableFuture<RaftNode>> futures = new ArrayList<>();
            for (RaftNode n : nodes) {
                if (allNodes.get(n.nodeId) == null) {
                    CompletableFuture<Peer> f = nioClient.addPeer(n.hostPort);
                    futures.add(f.thenApply(peer -> new RaftNode(n.nodeId, n.hostPort, peer)));
                }
            }
            boolean success = false;
            try {
                DtTime timeout = new DtTime(10, TimeUnit.SECONDS);
                for (CompletableFuture<RaftNode> f : futures) {
                    RaftNode n = f.get(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                    allNodes.put(n.nodeId, n);
                }
                success = true;
            } catch (InterruptedException e) {
                DtUtil.restoreInterruptStatus();
                throw new NetException(e);
            } catch (Exception e) {
                throw new NetException(e);
            } finally {
                if (!success) {
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
        Objects.requireNonNull(serverIds);
        if (serverIds.length == 0) {
            throw new IllegalArgumentException("servers is empty");
        }
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
        List<RaftNode> managedServers = new ArrayList<>();
        GroupInfo oldGroupInfo = groups.get(groupId);
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

        GroupInfo gi;
        if (oldGroupInfo != null && oldGroupInfo.leaderFuture != null) {
            gi = new GroupInfo(groupId, Collections.unmodifiableList(managedServers), leader, true);
            findLeader(gi, gi.servers.iterator());
            // use new leader future to complete the old one
            //noinspection DataFlowIssue
            gi.leaderFuture.whenComplete((result, ex) -> {
                if (ex != null) {
                    oldGroupInfo.leaderFuture.completeExceptionally(ex);
                } else {
                    oldGroupInfo.leaderFuture.complete(result);
                }
            });
        } else {
            gi = new GroupInfo(groupId, Collections.unmodifiableList(managedServers), leader, false);
        }
        groups.put(groupId, gi);
    }

    public void clientRemoveGroup(int groupId) throws NetException {
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

    public <T> void sendRequest(Integer groupId, WritePacket request, DecoderCallbackCreator<T> decoder,
                                DtTime timeout, RpcCallback<T> callback) {
        GroupInfo groupInfo = groups.get(groupId);
        if (groupInfo == null) {
            FutureCallback.callFail(callback, new NoSuchGroupException(groupId));
            return;
        }
        boolean getPermit = false;
        try {
            nioClient.getConfig().readFence();
            getPermit = nioClient.acquirePermit(request, timeout);
            if (groupInfo.leader != null && groupInfo.leader.peer.getStatus() == PeerStatus.connected
                    && (groupInfo.lastLeaderFailTime == 0 ||
                    System.currentTimeMillis() - groupInfo.lastLeaderFailTime < 2000)) {
                send(groupInfo, request, decoder, timeout, callback, 0, getPermit);
            } else {
                CompletableFuture<GroupInfo> leaderFuture;
                if (groupInfo.leaderFuture == null) {
                    leaderFuture = updateLeaderInfo(groupId);
                } else {
                    leaderFuture = groupInfo.leaderFuture;
                }
                final boolean finalGetPermit = getPermit;
                leaderFuture.whenComplete((gi, ex) -> {
                    if (ex != null) {
                        FutureCallback.callFail(callback, ex);
                        if (finalGetPermit) {
                            nioClient.releasePermit(request);
                        }
                    } else if (timeout.isTimeout()) {
                        FutureCallback.callFail(callback, new RaftTimeoutException("timeout after find leader for group " + groupId));
                        if (finalGetPermit) {
                            nioClient.releasePermit(request);
                        }
                    } else {
                        send(gi, request, decoder, timeout, callback, 0, finalGetPermit);
                    }
                });
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                DtUtil.restoreInterruptStatus();
            }
            FutureCallback.callFail(callback, e);
            if (getPermit) {
                nioClient.releasePermit(request);
            }
        }
    }

    private <T> void send(GroupInfo groupInfo, WritePacket request, DecoderCallbackCreator<T> decoder,
                          DtTime timeout, RpcCallback<T> c, int retry, boolean getPermit) {
        RpcCallback<T> newCallback = (result, ex) -> {
            if (ex instanceof NetCodeException) {
                NetCodeException ncEx = (NetCodeException) ex;
                if (ncEx.getCode() == CmdCodes.NOT_RAFT_LEADER && request.canRetry()
                        && retry == 0 && ncEx.getExtra() != null) {
                    GroupInfo newGroupInfo = updateLeaderFromExtra(ncEx.getExtra(), groupInfo);
                    if (newGroupInfo != null && newGroupInfo.leader != null && !timeout.isTimeout()) {
                        log.info("leader changed, update leader from node {} to {}, request will auto retry",
                                groupInfo.leader.nodeId, newGroupInfo.leader.nodeId);
                        request.prepareRetry();
                        send(newGroupInfo, request, decoder, timeout, c, 1, getPermit);
                        return;
                    }
                }
                if (ncEx.getCode() != CmdCodes.CLIENT_ERROR) {
                    updateLeaderFailTime(groupInfo);
                }
            } else if (ex != null) {
                updateLeaderFailTime(groupInfo);
            }
            if (getPermit) {
                nioClient.releasePermit(request);
            }
            if (c != null) {
                c.call(result, ex);
            }
        };
        nioClient.sendRequest(groupInfo.leader.peer, request, decoder, timeout, newCallback);
    }

    private void updateLeaderFailTime(GroupInfo oldGroupInfo) {
        GroupInfo currentGroupInfo = groups.get(oldGroupInfo.groupId);
        if (currentGroupInfo != oldGroupInfo) {
            return;
        }
        if (currentGroupInfo.lastLeaderFailTime != 0) {
            return;
        }
        lock.lock();
        try {
            currentGroupInfo = groups.get(oldGroupInfo.groupId);
            if (currentGroupInfo != oldGroupInfo) {
                return;
            }
            GroupInfo newGroupInfo = new GroupInfo(oldGroupInfo, System.currentTimeMillis());
            groups.put(oldGroupInfo.groupId, newGroupInfo);
        } finally {
            lock.unlock();
        }
    }

    private GroupInfo updateLeaderFromExtra(byte[] extra, GroupInfo groupInfo) {
        GroupInfo currentGroupInfo = groups.get(groupInfo.groupId);
        if (currentGroupInfo != groupInfo) {
            // group info changed, drop the result
            return null;
        }
        lock.lock();
        try {
            currentGroupInfo = groups.get(groupInfo.groupId);
            if (currentGroupInfo != groupInfo) {
                // group info changed, drop the result
                return null;
            }
            String s = new String(extra, StandardCharsets.UTF_8);
            RaftNode leader = parseLeader(groupInfo, Integer.parseInt(s));
            if (leader != null) {
                GroupInfo newGroupInfo = new GroupInfo(groupInfo, leader, false);
                groups.put(groupInfo.groupId, newGroupInfo);
                return newGroupInfo;
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<RaftNode> fetchLeader(int groupId) {
        return updateLeaderInfo(groupId).thenApply(gi -> gi.leader);
    }

    protected CompletableFuture<GroupInfo> updateLeaderInfo(Integer groupId) {
        lock.lock();
        try {
            GroupInfo gi = groups.get(groupId);
            if (gi == null) {
                return DtUtil.failedFuture(new NoSuchGroupException(groupId));
            }
            if (gi.leader != null && gi.leader.peer.getStatus() == PeerStatus.connected && gi.lastLeaderFailTime == 0) {
                return CompletableFuture.completedFuture(gi);
            }
            if (gi.leaderFuture != null) {
                return gi.leaderFuture;
            }
            GroupInfo newGroupInfo = new GroupInfo(gi, null, true);
            groups.put(groupId, newGroupInfo);
            Iterator<RaftNode> it = newGroupInfo.servers.iterator();
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

            // set new group info, to trigger next find
            GroupInfo newGroupInfo = new GroupInfo(gi, null, false);
            groups.put(gi.groupId, newGroupInfo);
            return;
        }
        RaftNode node = it.next();
        CompletableFuture<QueryStatusResp> s = queryRaftServerStatus(node.nodeId, gi.groupId);
        s.whenComplete((resp, ex) -> processLeaderQueryResult(gi, it, resp, ex, node));
    }

    private void processLeaderQueryResult(GroupInfo gi, Iterator<RaftNode> it, QueryStatusResp status,
                                          Throwable ex, RaftNode node) {
        lock.lock();
        try {
            GroupInfo currentGroupInfo = groups.get(gi.groupId);
            assert gi.leaderFuture != null;
            if (currentGroupInfo == null) {
                gi.leaderFuture.completeExceptionally(new RaftException("group removed " + gi.groupId));
                return;
            }
            if (currentGroupInfo != gi) {
                // group info changed, stop current find process
                updateLeaderInfo(gi.groupId).whenComplete((ld, e) -> {
                    if (e != null) {
                        gi.leaderFuture.completeExceptionally(e);
                    } else {
                        gi.leaderFuture.complete(ld);
                    }
                });
                return;
            }
            if (ex != null) {
                log.warn("query leader from {} fail: {}", node.peer.getEndPoint(), ex.toString());
                findLeader(gi, it);
            } else {
                if (status == null || status.getLeaderId() <= 0) {
                    log.error("query leader from {} fail, leader id illegal: {}",
                            node.peer.getEndPoint(), status == null ? null : status.getLeaderId());
                    findLeader(gi, it);
                } else {
                    RaftNode leader = parseLeader(gi, status.getLeaderId());
                    if (leader != null) {
                        log.debug("find leader for group {}: {}", gi.groupId, leader.peer.getEndPoint());
                        try {
                            nioClient.connect(leader.peer, createDefaultTimeout())
                                    .whenComplete((v, e) -> connectToLeaderCallback(gi, leader, e));
                        } catch (Exception e) {
                            log.error("", e);
                            connectToLeaderCallback(gi, leader, e);
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
        assert gi.leaderFuture != null;
        lock.lock();
        try {
            Peer leaderPeer = leader.peer;
            if (e != null) {
                log.warn("connect to leader {} fail: {}", leaderPeer.getEndPoint(), e.toString());
                groups.put(gi.groupId, new GroupInfo(gi, null, false));
                RaftException re = new RaftException("connect to leader " + leaderPeer.getEndPoint() + " fail");
                gi.leaderFuture.completeExceptionally(re);
            } else {
                log.info("group {} connected to leader: {}", gi.groupId, leaderPeer.getEndPoint());
                GroupInfo newGroupInfo = new GroupInfo(gi, leader, false);
                groups.put(gi.groupId, newGroupInfo);
                gi.leaderFuture.complete(newGroupInfo);
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
        nioClient.waitStart();
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
        nioClient.stop(timeout);
    }

    public NioClient getNioClient() {
        return nioClient;
    }

    public DtTime createDefaultTimeout() {
        return new DtTime(rpcTimeoutMillis, TimeUnit.MILLISECONDS);
    }
}
