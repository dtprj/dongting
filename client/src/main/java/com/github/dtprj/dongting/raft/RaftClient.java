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
import com.github.dtprj.dongting.common.IntObjMap;
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
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.WritePacket;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
@SuppressWarnings("Convert2Diamond")
public class RaftClient extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftClient.class);
    private final NioClient nioClient;
    // key is nodeId
    private final IntObjMap<RaftNode> allNodes = new IntObjMap<>();
    // key is groupId
    private final ConcurrentHashMap<Integer, GroupInfo> groups = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();
    private long nextEpoch = 0;

    public RaftClient(NioClientConfig nioClientConfig) {
        this.nioClient = new NioClient(nioClientConfig);
    }

    public void addOrUpdateGroup(int groupId, String servers) throws NetException {
        List<RaftNode> list = RaftNode.parseServers(servers);
        addOrUpdateGroup(groupId, list);
    }

    public void addOrUpdateGroup(int groupId, List<RaftNode> servers) throws NetException {
        Objects.requireNonNull(servers);
        if (servers.isEmpty()) {
            throw new IllegalArgumentException("servers is empty");
        }
        lock.lock();
        try {
            addOrUpdateGroupInLock(groupId, servers);
        } finally {
            lock.unlock();
        }
    }

    private void addOrUpdateGroupInLock(int groupId, List<RaftNode> servers) throws NetException {
        ArrayList<RaftNode> needAddList = new ArrayList<>();
        ArrayList<RaftNode> managedServers = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (RaftNode n : servers) {
            Objects.requireNonNull(n);
            RaftNode existManagedNode = allNodes.get(n.getNodeId());
            if (existManagedNode == null) {
                CompletableFuture<Peer> f = nioClient.addPeer(n.getHostPort());
                futures.add(f.thenAccept(peer -> {
                    RaftNode newNode = new RaftNode(n.getNodeId(), n.getHostPort(), peer);
                    needAddList.add(newNode);
                    managedServers.add(newNode);
                }));
            } else {
                managedServers.add(existManagedNode);
            }
        }
        if (!futures.isEmpty()) {
            boolean success = false;
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
                success = true;
            } catch (InterruptedException e) {
                DtUtil.restoreInterruptStatus();
                throw new NetException(e);
            } catch (Exception e) {
                throw new NetException(e);
            } finally {
                if (!success) {
                    for (RaftNode n : needAddList) {
                        nioClient.removePeer(n.getHostPort());
                    }
                }
            }
        }

        GroupInfo oldGroupInfo = groups.get(groupId);
        RaftNode leader = null;
        for (RaftNode n : managedServers) {
            n.useCount++;
            if (oldGroupInfo != null) {
                RaftNode oldLeader = oldGroupInfo.leader;
                if (oldLeader != null && oldLeader == n) {
                    // old leader in the new servers list
                    leader = oldLeader;
                }
            }
        }

        releaseOldGroup(oldGroupInfo);

        GroupInfo gi;
        if (oldGroupInfo != null && oldGroupInfo.leaderFuture != null) {
            gi = new GroupInfo(groupId, generateNextEpoch(), Collections.unmodifiableList(managedServers),
                    leader, true);
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
            gi = new GroupInfo(groupId, generateNextEpoch(), Collections.unmodifiableList(managedServers),
                    leader, false);
        }
        groups.put(groupId, gi);
    }

    private long generateNextEpoch() {
        return nextEpoch++;
    }

    private void releaseOldGroup(GroupInfo oldGroupInfo) {
        if (oldGroupInfo != null) {
            for (RaftNode n : oldGroupInfo.servers) {
                n.useCount--;
                if (n.useCount <= 0) {
                    allNodes.remove(n.getNodeId());
                    nioClient.removePeer(n.getPeer());
                }
            }
        }
    }

    @SuppressWarnings("unused")
    public void removeGroup(int groupId) throws NetException {
        lock.lock();
        try {
            GroupInfo oldGroupInfo = groups.remove(groupId);
            if (oldGroupInfo != null) {
                if (oldGroupInfo.leaderFuture != null) {
                    oldGroupInfo.leaderFuture.completeExceptionally(new RaftException("group removed " + groupId));
                }
                releaseOldGroup(oldGroupInfo);
            }
        } finally {
            lock.unlock();
        }
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
            getPermit = nioClient.acquirePermit(request, timeout);
            if (groupInfo.leader != null && groupInfo.leader.getPeer().getStatus() == PeerStatus.connected) {
                send(groupId, request, decoder, timeout, callback, groupInfo.leader, 0, getPermit);
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
                    } else if (gi == null || gi.leader == null) {
                        FutureCallback.callFail(callback, new RaftException("can't find leader for group " + groupId));
                        if (finalGetPermit) {
                            nioClient.releasePermit(request);
                        }
                    } else if (timeout.isTimeout()) {
                        FutureCallback.callFail(callback, new RaftTimeoutException("timeout after find leader for group " + groupId));
                        if (finalGetPermit) {
                            nioClient.releasePermit(request);
                        }
                    } else {
                        send(groupId, request, decoder, timeout, callback, gi.leader, 0, finalGetPermit);
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

    private <T> void send(Integer groupId, WritePacket request, DecoderCallbackCreator<T> decoder,
                          DtTime timeout, RpcCallback<T> c, RaftNode leader, int retry, boolean getPermit) {
        RpcCallback<T> newCallback = new RpcCallback<T>() {
            @Override
            public void success(ReadPacket<T> resp) {
                if (c != null) {
                    c.success(resp);
                }
                if (getPermit) {
                    nioClient.releasePermit(request);
                }
            }

            @Override
            public void fail(Throwable ex) {
                if (request.canRetry() && retry == 0 && ex instanceof NetCodeException) {
                    NetCodeException ncEx = (NetCodeException) ex;
                    if (ncEx.getCode() == CmdCodes.NOT_RAFT_LEADER) {
                        GroupInfo gi = groups.get(groupId);
                        if (gi != null) {
                            RaftNode newLeader = updateLeaderFromExtra(ncEx.getExtra(), gi);
                            if (newLeader != null && !timeout.isTimeout()) {
                                request.prepareRetry();
                                send(groupId, request, decoder, timeout, c, newLeader, 1, getPermit);
                                return;
                            }
                        }
                    }
                }
                if (c != null) {
                    c.fail(ex);
                    if (getPermit) {
                        nioClient.releasePermit(request);
                    }
                }
            }
        };
        nioClient.sendRequest(leader.getPeer(), request, decoder, timeout, newCallback);
    }

    private RaftNode updateLeaderFromExtra(byte[] extra, GroupInfo groupInfo) {
        if (extra == null) {
            return null;
        }
        lock.lock();
        try {
            GroupInfo currentGroupInfo = groups.get(groupInfo.groupId);
            if (currentGroupInfo == null || currentGroupInfo.epoch != groupInfo.epoch) {
                // group info changed, drop the result
                return null;
            }
            String s = new String(extra, StandardCharsets.UTF_8);
            RaftNode leader = parseLeader(groupInfo, Integer.parseInt(s));
            if (leader != null) {
                GroupInfo newGroupInfo = new GroupInfo(groupInfo.groupId, groupInfo.epoch,
                        groupInfo.servers, leader, false);
                groups.put(groupInfo.groupId, newGroupInfo);
            }
            return leader;
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<RaftNode> fetchLeader(int groupId) {
        return updateLeaderInfo(groupId).thenApply(gi -> {
            if (gi == null || gi.leader == null) {
                return null;
            } else {
                return gi.leader;
            }
        });
    }

    private CompletableFuture<GroupInfo> updateLeaderInfo(int groupId) {
        lock.lock();
        try {
            GroupInfo gi = groups.get(groupId);
            if (gi == null) {
                return DtUtil.failedFuture(new NoSuchGroupException(groupId));
            }
            if (gi.leader != null && gi.leader.getPeer().getStatus() == PeerStatus.connected) {
                return CompletableFuture.completedFuture(gi);
            }
            if (gi.leaderFuture != null) {
                return gi.leaderFuture;
            }
            GroupInfo newGroupInfo = new GroupInfo(groupId, generateNextEpoch(), gi.servers, null, true);
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
            GroupInfo newGroupInfo = new GroupInfo(gi.groupId, gi.epoch, gi.servers, null, false);
            groups.put(gi.groupId, newGroupInfo);
            return;
        }
        RaftNode node = it.next();
        PbIntWritePacket req = new PbIntWritePacket(Commands.RAFT_QUERY_STATUS, gi.groupId);
        DtTime rpcTimeout = new DtTime(5, TimeUnit.SECONDS);
        DecoderCallbackCreator<QueryStatusResp> dc = c -> c.toDecoderCallback(new QueryStatusResp.QueryStatusRespCallback());
        RpcCallback<QueryStatusResp> callback = RpcCallback.fromHandler(
                (resp, ex) -> processLeaderQueryResult(gi, it, resp, ex, node));
        nioClient.sendRequest(node.getPeer(), req, dc, rpcTimeout, callback);
    }

    private void processLeaderQueryResult(GroupInfo gi, Iterator<RaftNode> it,
                                          ReadPacket<QueryStatusResp> rf, Throwable ex, RaftNode node) {
        lock.lock();
        try {
            GroupInfo currentGroupInfo = groups.get(gi.groupId);
            assert gi.leaderFuture != null;
            if (currentGroupInfo == null) {
                gi.leaderFuture.completeExceptionally(new RaftException("group removed " + gi.groupId));
                return;
            }
            if (currentGroupInfo.epoch != gi.epoch) {
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
                log.warn("query leader from {} fail: {}", node.getPeer().getEndPoint(), ex.toString());
                findLeader(gi, it);
            } else {
                if (rf.getBody() == null || rf.getBody().getLeaderId() <= 0) {
                    log.error("query leader from {} fail, leader id illegal: {}",
                            node.getPeer().getEndPoint(), rf.getBody() == null ? null : rf.getBody().getLeaderId());
                    findLeader(gi, it);
                } else {
                    RaftNode leader = parseLeader(gi, rf.getBody().getLeaderId());
                    if (leader != null) {
                        log.debug("find leader for group {}: {}", gi.groupId, leader.getPeer().getEndPoint());
                        connectToLeader(gi, leader);
                    } else {
                        findLeader(gi, it);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void connectToLeader(GroupInfo gi, RaftNode leader) {
        assert gi.leaderFuture != null;
        try {
            Peer leaderPeer = leader.getPeer();
            nioClient.connect(leaderPeer, new DtTime(5, TimeUnit.SECONDS)).whenComplete((v, e) -> {
                if (e != null) {
                    log.warn("connect to leader {} fail: {}", leaderPeer.getEndPoint(), e.toString());
                    gi.leaderFuture.completeExceptionally(new RaftException("connect to leader " + leaderPeer.getEndPoint() + " fail"));
                } else {
                    log.info("group {} connected to leader: {}", gi.groupId, leaderPeer.getEndPoint());
                    GroupInfo newGroupInfo = new GroupInfo(gi.groupId, gi.epoch, gi.servers, leader, false);
                    groups.put(gi.groupId, newGroupInfo);
                    gi.leaderFuture.complete(newGroupInfo);
                }
            });
        } catch (Exception e) {
            log.error("", e);
            gi.leaderFuture.completeExceptionally(e);
        }
    }

    private RaftNode parseLeader(GroupInfo groupInfo, int leaderId) {
        for (RaftNode ni : groupInfo.servers) {
            if (ni.getNodeId() == leaderId) {
                return ni;
            }
        }
        log.warn("leader {} not in group {}", leaderId, groupInfo.groupId);
        return null;
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
}
