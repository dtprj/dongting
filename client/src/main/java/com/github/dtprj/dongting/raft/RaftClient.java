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

import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.PbNoCopyDecoder;
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
import com.github.dtprj.dongting.net.PbIntWriteFrame;
import com.github.dtprj.dongting.net.Peer;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.WriteFrame;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public class RaftClient extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftClient.class);
    private final NioClient client;
    // key is nodeId
    private final IntObjMap<NodeInfo> allNodes = new IntObjMap<>();
    // key is groupId
    private volatile IntObjMap<GroupInfo> groups = new IntObjMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    public RaftClient(NioClientConfig nioClientConfig) {
        this.client = new NioClient(nioClientConfig);
    }

    public void addOrUpdateGroup(int groupId, List<RaftNode> servers) throws NetException {
        lock.lock();
        try {
            addOrUpdateGroupInLock(groupId, servers);
        } finally {
            lock.unlock();
        }
    }

    private void addOrUpdateGroupInLock(int groupId, List<RaftNode> servers) throws NetException {
        Objects.requireNonNull(servers);
        if (servers.isEmpty()) {
            throw new IllegalArgumentException("servers is empty");
        }

        ArrayList<NodeInfo> needAddList = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (RaftNode n : servers) {
            Objects.requireNonNull(n);
            if (allNodes.get(n.getNodeId()) == null) {
                CompletableFuture<Peer> f = client.addPeer(n.getHostPort());
                futures.add(f.thenAccept(peer -> needAddList.add(new NodeInfo(n.getNodeId(), n.getHostPort(), peer))));
            }
        }
        if (!needAddList.isEmpty()) {
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

        ArrayList<NodeInfo> nodeInfoOfGroup = new ArrayList<>();
        GroupInfo oldGroupInfo = groups.get(groupId);
        Peer leader = null;
        for (RaftNode n : servers) {
            NodeInfo nodeInfo = allNodes.get(n.getNodeId());
            if (nodeInfo != null) {
                nodeInfo.getRefCount().retain();
                nodeInfoOfGroup.add(nodeInfo);
            }
            if (oldGroupInfo != null) {
                Peer oldLeader = oldGroupInfo.getLeader();
                if (oldLeader != null && oldLeader.getEndPoint().equals(n.getHostPort())) {
                    // old leader in the new servers list
                    leader = oldLeader;
                }
            }
        }
        for (NodeInfo nodeInfo : needAddList) {
            nodeInfoOfGroup.add(nodeInfo);
            allNodes.put(nodeInfo.getNodeId(), nodeInfo);
        }

        releaseOldGroup(oldGroupInfo);

        GroupInfo gi = new GroupInfo(groupId, nodeInfoOfGroup, leader, null);
        //noinspection NonAtomicOperationOnVolatileField
        this.groups = IntObjMap.copyOnWritePut(groups, groupId, gi).getRight();
    }

    private void releaseOldGroup(GroupInfo oldGroupInfo) {
        if (oldGroupInfo != null) {
            for (NodeInfo nodeInfo : oldGroupInfo.getServers()) {
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
            GroupInfo oldGroupInfo = pair.getLeft();
            this.groups = pair.getRight();

            releaseOldGroup(oldGroupInfo);
        } finally {
            lock.unlock();
        }
    }

    public <T> CompletableFuture<ReadFrame<T>> sendRequest(int groupId, WriteFrame request,
                                                           Decoder<T> decoder, DtTime timeout) {
        GroupInfo groupInfo = groups.get(groupId);
        if (groupInfo == null) {
            return DtUtil.failedFuture(new NoSuchGroupException(groupId));
        }
        CompletableFuture<ReadFrame<T>> finalResult = new CompletableFuture<>();
        CompletableFuture<ReadFrame<T>> result;
        if (groupInfo.getLeader() != null) {
            result = client.sendRequest(groupInfo.getLeader(), request, decoder, timeout);
        } else {
            CompletableFuture<Peer> leaderFuture;
            if (groupInfo.getLeaderFuture() == null) {
                leaderFuture = updateLeaderInfo(groupId);
            } else {
                leaderFuture = groupInfo.getLeaderFuture();
            }
            result = leaderFuture.thenCompose(leader -> {
                if (leader == null) {
                    return DtUtil.failedFuture(new RaftException("no leader"));
                }
                if (timeout.isTimeout()) {
                    return DtUtil.failedFuture(new NetTimeoutException("timeout after find leader"));
                }
                return client.sendRequest(leader, request, decoder, timeout);
            });
        }
        result.whenComplete((rf, ex) -> {
            if (ex == null) {
                finalResult.complete(rf);
                return;
            }
            if (ex instanceof CompletionException) {
                ex = ex.getCause();
            }
            if (ex instanceof NetCodeException) {
                NetCodeException ncEx = (NetCodeException) ex;
                if (ncEx.getCode() == CmdCodes.NOT_RAFT_LEADER) {
                    Peer newLeader = updateLeaderFromExtra(ncEx.getRespFrame(), groupInfo);
                    if (newLeader != null && !timeout.isTimeout()) {
                        client.sendRequest(newLeader, request, decoder, timeout).whenComplete((rf2, ex2) -> {
                            if (ex2 == null) {
                                finalResult.complete(rf2);
                            } else {
                                finalResult.completeExceptionally(ex2);
                            }
                        });
                        return;
                    }
                }
            }
            finalResult.completeExceptionally(ex);
        });
        return finalResult;
    }

    private CompletableFuture<Peer> updateLeaderInfo(int groupId) {
        lock.lock();
        try {
            GroupInfo gi = groups.get(groupId);
            if (gi == null) {
                return DtUtil.failedFuture(new NoSuchGroupException(groupId));
            }
            if (gi.getLeader() != null) {
                return CompletableFuture.completedFuture(gi.getLeader());
            }
            if (gi.getLeaderFuture() != null) {
                return gi.getLeaderFuture();
            }
            CompletableFuture<Peer> f = new CompletableFuture<>();
            GroupInfo newGroupInfo = new GroupInfo(groupId, gi.getServers(), null, f);
            groups.put(groupId, newGroupInfo);
            Iterator<NodeInfo> it = newGroupInfo.getServers().iterator();
            findLeader(newGroupInfo, it);
            return f;
        } finally {
            lock.unlock();
        }
    }

    private void findLeader(GroupInfo groupInfo, Iterator<NodeInfo> it) {
        GroupInfo currentGroupInfo = groups.get(groupInfo.getGroupId());
        if (currentGroupInfo != groupInfo) {
            // group info changed, stop current find process
            if (currentGroupInfo.getLeader() != null) {
                groupInfo.getLeaderFuture().complete(currentGroupInfo.getLeader());
            } else {
                CompletableFuture<Peer> newLeaderFuture = currentGroupInfo.getLeaderFuture();
                if (newLeaderFuture == null) {
                    newLeaderFuture = updateLeaderInfo(groupInfo.getGroupId());
                }
                // use new leader future to complete the old one
                newLeaderFuture.whenComplete((leader, ex) -> {
                    if (ex != null) {
                        groupInfo.getLeaderFuture().completeExceptionally(ex);
                    } else {
                        groupInfo.getLeaderFuture().complete(leader);
                    }
                });
            }
            return;
        }
        if (!it.hasNext()) {
            groupInfo.getLeaderFuture().complete(null);

            // set new group info, to trigger next find
            GroupInfo newGroupInfo = new GroupInfo(groupInfo.getGroupId(),
                    groupInfo.getServers(), null, null);
            groups.put(groupInfo.getGroupId(), newGroupInfo);
            return;
        }
        NodeInfo node = it.next();
        PbIntWriteFrame req = new PbIntWriteFrame(groupInfo.getGroupId());
        req.setCommand(Commands.RAFT_QUERY_LEADER);
        DtTime rpcTimeout = new DtTime(3, TimeUnit.SECONDS);
        client.sendRequest(node.getPeer(), req, PbNoCopyDecoder.SIMPLE_INT_DECODER, rpcTimeout)
                .whenComplete((rf, ex) -> processLeaderQueryResult(groupInfo, it, rf, ex, node));
    }

    private void processLeaderQueryResult(GroupInfo groupInfo, Iterator<NodeInfo> it,
                                          ReadFrame<Integer> rf, Throwable ex, NodeInfo node) {
        lock.lock();
        try {
            if (ex != null) {
                log.warn("query leader from {} fail: {}", node.getPeer().getEndPoint(), ex.toString());
                findLeader(groupInfo, it);
            } else {
                if (rf.getBody() == null || rf.getBody() < 0) {
                    log.error("query leader from {} fail, leader id illegal: {}", node.getPeer().getEndPoint(), rf.getBody());
                    findLeader(groupInfo, it);
                } else {
                    Peer leader = parseLeader(groupInfo, rf.getBody());
                    if (leader != null) {
                        groupInfo.getLeaderFuture().complete(leader);
                    } else {
                        findLeader(groupInfo, it);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private Peer updateLeaderFromExtra(ReadFrame<?> frame, GroupInfo groupInfo) {
        lock.lock();
        try {
            GroupInfo currentGroupInfo = groups.get(groupInfo.getGroupId());
            if (currentGroupInfo != groupInfo) {
                // group info changed, drop the result
                return null;
            }
            byte[] bs = frame.getExtra();
            if (bs == null) {
                return null;
            }
            String s = new String(bs, StandardCharsets.UTF_8);
            Peer leader = parseLeader(groupInfo, Integer.parseInt(s));
            if (leader != null) {
                GroupInfo newGroupInfo = new GroupInfo(groupInfo.getGroupId(),
                        groupInfo.getServers(), leader, null);
                groups.put(groupInfo.getGroupId(), newGroupInfo);
            }
            return leader;
        } finally {
            lock.unlock();
        }
    }

    private Peer parseLeader(GroupInfo groupInfo, int leaderId) {
        for (NodeInfo ni : groupInfo.getServers()) {
            if (ni.getNodeId() == leaderId) {
                log.info("group {} find leader: {}, {}", groupInfo.getGroupId(), leaderId, ni.getHostPort());
                return ni.getPeer();
            }
        }
        log.warn("leader {} not in group {}", leaderId, groupInfo.getGroupId());
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
