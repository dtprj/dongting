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

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.NetException;
import com.github.dtprj.dongting.net.NetTimeoutException;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.PbIntWritePacket;
import com.github.dtprj.dongting.net.Peer;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RetryableWritePacket;
import com.github.dtprj.dongting.net.RpcCallback;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * @author huangli
 */
@SuppressWarnings("Convert2Diamond")
public class RaftClient extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftClient.class);
    private final NioClient client;
    // key is nodeId
    private final IntObjMap<NodeInfo> allNodes = new IntObjMap<>();
    // key is groupId
    private volatile IntObjMap<GroupInfo> groups = new IntObjMap<>();

    private final ReentrantLock lock = new ReentrantLock();
    private long nextEpoch = 0;

    public RaftClient(NioClientConfig nioClientConfig) {
        this.client = new NioClient(nioClientConfig);
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
        ArrayList<NodeInfo> needAddList = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (RaftNode n : servers) {
            Objects.requireNonNull(n);
            if (allNodes.get(n.getNodeId()) == null) {
                CompletableFuture<Peer> f = client.addPeer(n.getHostPort());
                futures.add(f.thenAccept(peer -> needAddList.add(new NodeInfo(n.getNodeId(), n.getHostPort(), peer))));
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
                    for (NodeInfo n : needAddList) {
                        client.removePeer(n.getHostPort());
                    }
                }
            }
        }

        ArrayList<NodeInfo> nodeInfoList = new ArrayList<>();
        GroupInfo oldGroupInfo = groups.get(groupId);
        Peer leader = null;
        for (RaftNode n : servers) {
            NodeInfo nodeInfo = allNodes.get(n.getNodeId());
            if (nodeInfo != null) {
                nodeInfo.getRefCount().retain();
                nodeInfoList.add(nodeInfo);
            }
            if (oldGroupInfo != null) {
                Peer oldLeader = oldGroupInfo.leader;
                if (oldLeader != null && oldLeader.getEndPoint().equals(n.getHostPort())) {
                    // old leader in the new servers list
                    leader = oldLeader;
                }
            }
        }
        for (NodeInfo nodeInfo : needAddList) {
            nodeInfoList.add(nodeInfo);
            allNodes.put(nodeInfo.getNodeId(), nodeInfo);
        }

        releaseOldGroup(oldGroupInfo);

        GroupInfo gi;
        if (oldGroupInfo != null && oldGroupInfo.leaderFuture != null) {
            gi = new GroupInfo(groupId, generateNextEpoch(), nodeInfoList, leader, true);
            findLeader(gi, nodeInfoList.iterator());
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
            gi = new GroupInfo(groupId, generateNextEpoch(), nodeInfoList, leader, false);
        }
        //noinspection NonAtomicOperationOnVolatileField
        this.groups = IntObjMap.copyOnWritePut(groups, groupId, gi).getRight();
    }

    private long generateNextEpoch() {
        return nextEpoch++;
    }

    private void releaseOldGroup(GroupInfo oldGroupInfo) {
        if (oldGroupInfo != null) {
            for (NodeInfo nodeInfo : oldGroupInfo.servers) {
                if (nodeInfo.getRefCount().release()) {
                    allNodes.remove(nodeInfo.getNodeId());
                    client.removePeer(nodeInfo.getPeer());
                }
            }
        }
    }

    @SuppressWarnings("unused")
    public void removeGroup(int groupId) throws NetException {
        lock.lock();
        try {
            Pair<GroupInfo, IntObjMap<GroupInfo>> pair = IntObjMap.copyOnWriteRemove(groups, groupId);
            this.groups = pair.getRight();
            GroupInfo oldGroupInfo = pair.getLeft();
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

    public <T> void sendRequest(int groupId, RetryableWritePacket request,
                                Function<DecodeContext, DecoderCallback<T>> decoderCallback, DtTime timeout,
                                RpcCallback<T> callback) {
        GroupInfo groupInfo = groups.get(groupId);
        if (groupInfo == null) {
            RpcCallback.callFail(callback, new NoSuchGroupException(groupId));
            return;
        }
        if (groupInfo.leader != null) {
            send(request, decoderCallback, timeout, callback, groupInfo, true);
        } else {
            CompletableFuture<GroupInfo> leaderFuture;
            if (groupInfo.leaderFuture == null) {
                leaderFuture = updateLeaderInfo(groupId);
            } else {
                leaderFuture = groupInfo.leaderFuture;
            }
            leaderFuture.whenComplete((gi, ex) -> {
                if (ex != null) {
                    RpcCallback.callFail(callback, ex);
                } else if (gi == null) {
                    RpcCallback.callFail(callback, new RaftException("no leader"));
                } else if (timeout.isTimeout()) {
                    RpcCallback.callFail(callback, new NetTimeoutException("timeout after find leader"));
                } else {
                    send(request, decoderCallback, timeout, callback, gi, true);
                }
            });
        }
    }

    private <T> void send(RetryableWritePacket request, Function<DecodeContext, DecoderCallback<T>> decoderCallback,
                          DtTime timeout, RpcCallback<T> c, GroupInfo gi, boolean retry) {
        RpcCallback<T> newCallback = new RpcCallback<T>() {
            @Override
            public void success(ReadPacket<T> resp) {
                c.success(resp);
            }

            @Override
            public void fail(Throwable ex) {
                if (retry && ex instanceof NetCodeException) {
                    NetCodeException ncEx = (NetCodeException) ex;
                    if (ncEx.getCode() == CmdCodes.NOT_RAFT_LEADER) {
                        Peer newLeader = updateLeaderFromExtra(ncEx.getRespPacket(), gi);
                        if (newLeader != null && !timeout.isTimeout()) {
                            request.reset();
                            send(request, decoderCallback, timeout, c, gi, false);
                            return;
                        }
                    }
                }
                c.fail(ex);
            }
        };
        client.sendRequest(gi.leader, request, decoderCallback, timeout, newCallback);
    }

    private CompletableFuture<GroupInfo> updateLeaderInfo(int groupId) {
        lock.lock();
        try {
            GroupInfo gi = groups.get(groupId);
            if (gi == null) {
                return DtUtil.failedFuture(new NoSuchGroupException(groupId));
            }
            if (gi.leader != null) {
                return CompletableFuture.completedFuture(gi);
            }
            if (gi.leaderFuture != null) {
                return gi.leaderFuture;
            }
            GroupInfo newGroupInfo = new GroupInfo(groupId, generateNextEpoch(), gi.servers, null, true);
            groups.put(groupId, newGroupInfo);
            Iterator<NodeInfo> it = newGroupInfo.servers.iterator();
            findLeader(newGroupInfo, it);
            return newGroupInfo.leaderFuture;
        } finally {
            lock.unlock();
        }
    }

    private void findLeader(GroupInfo gi, Iterator<NodeInfo> it) {
        if (!it.hasNext()) {
            //noinspection DataFlowIssue
            gi.leaderFuture.complete(null);

            // set new group info, to trigger next find
            GroupInfo newGroupInfo = new GroupInfo(gi.groupId, gi.epoch, gi.servers, null, false);
            groups.put(gi.groupId, newGroupInfo);
            return;
        }
        NodeInfo node = it.next();
        PbIntWritePacket req = new PbIntWritePacket(Commands.RAFT_QUERY_STATUS, gi.groupId);
        DtTime rpcTimeout = new DtTime(3, TimeUnit.SECONDS);
        client.sendRequest(node.getPeer(), req, c -> c.getOrCreatePbNoCopyDecoderCallback(new QueryStatusResp.QueryStatusRespCallback()), rpcTimeout)
                .whenComplete((rf, ex) -> processLeaderQueryResult(gi, it, rf, ex, node));
    }

    private void processLeaderQueryResult(GroupInfo gi, Iterator<NodeInfo> it,
                                          ReadPacket<QueryStatusResp> rf, Throwable ex, NodeInfo node) {
        lock.lock();
        try {
            GroupInfo currentGroupInfo = groups.get(gi.groupId);
            if (currentGroupInfo == null || currentGroupInfo.epoch != gi.epoch) {
                // group info changed, stop current find process
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
                    Peer leader = parseLeader(gi, rf.getBody().getLeaderId());
                    if (leader != null) {
                        GroupInfo newGroupInfo = new GroupInfo(gi.groupId, gi.epoch, gi.servers, leader, false);
                        groups.put(gi.groupId, newGroupInfo);
                        //noinspection DataFlowIssue
                        gi.leaderFuture.complete(newGroupInfo);
                    } else {
                        findLeader(gi, it);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private Peer updateLeaderFromExtra(ReadPacket<?> packet, GroupInfo groupInfo) {
        byte[] bs = packet.getExtra();
        if (bs == null) {
            return null;
        }
        lock.lock();
        try {
            GroupInfo currentGroupInfo = groups.get(groupInfo.groupId);
            if (currentGroupInfo == null || currentGroupInfo.epoch != groupInfo.epoch) {
                // group info changed, drop the result
                return null;
            }
            String s = new String(bs, StandardCharsets.UTF_8);
            Peer leader = parseLeader(groupInfo, Integer.parseInt(s));
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

    private Peer parseLeader(GroupInfo groupInfo, int leaderId) {
        for (NodeInfo ni : groupInfo.servers) {
            if (ni.getNodeId() == leaderId) {
                log.info("group {} find leader: {}, {}", groupInfo.groupId, leaderId, ni.getHostPort());
                return ni.getPeer();
            }
        }
        log.warn("leader {} not in group {}", leaderId, groupInfo.groupId);
        return null;
    }

    @Override
    protected void doStart() {
        client.start();
        client.waitStart();
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
        client.stop(timeout);
    }
}
