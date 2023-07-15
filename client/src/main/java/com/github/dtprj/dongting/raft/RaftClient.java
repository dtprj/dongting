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
import com.github.dtprj.dongting.common.RefCount;
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
import com.github.dtprj.dongting.net.NioNet;
import com.github.dtprj.dongting.net.PbIntWriteFrame;
import com.github.dtprj.dongting.net.Peer;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.WriteFrame;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author huangli
 */
public class RaftClient extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftClient.class);
    private final NioClient client;
    private final Map<HostPort, Pair<RefCount, Peer>> peers = new HashMap<>();
    private volatile IntObjMap<GroupInfo> groups = new IntObjMap<>();

    public RaftClient(NioClientConfig nioClientConfig) {
        this.client = new NioClient(nioClientConfig);
    }

    public synchronized void addOrUpdateGroup(int groupId, List<HostPort> servers) throws NetException {
        Objects.requireNonNull(servers);
        if (servers.size() == 0) {
            throw new IllegalArgumentException("servers is empty");
        }
        ArrayList<HostPort> needAddList = new ArrayList<>();
        for (HostPort hp : servers) {
            Objects.requireNonNull(hp);
            Pair<RefCount, Peer> peerInfo = peers.get(hp);
            if (peerInfo == null) {
                needAddList.add(hp);
            }
        }
        HashMap<HostPort, Pair<RefCount, Peer>> newPeers = new HashMap<>();
        if (needAddList.size() > 0) {
            boolean success = false;
            try {
                ArrayList<CompletableFuture<Peer>> futures = new ArrayList<>();
                for (HostPort hp : needAddList) {
                    futures.add(client.addPeer(hp));
                }
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
                for (CompletableFuture<Peer> f : futures) {
                    Peer peer = f.get();
                    newPeers.put(peer.getEndPoint(), new Pair<>(new RefCount(false), peer));
                }
                success = true;
            } catch (InterruptedException e) {
                DtUtil.restoreInterruptStatus();
                throw new NetException(e);
            } catch (ExecutionException e) {
                throw new NetException(e);
            } catch (TimeoutException e) {
                throw new NetTimeoutException("add peer timeout");
            } finally {
                if (!success) {
                    for (HostPort hp : needAddList) {
                        client.removePeer(hp);
                    }
                }
            }
        }

        ArrayList<Peer> allPeers = new ArrayList<>();
        for (HostPort hp : servers) {
            Pair<RefCount, Peer> peerInfo = peers.get(hp);
            if (peerInfo != null) {
                peerInfo.getLeft().retain();
                allPeers.add(peerInfo.getRight());
            }
        }
        peers.putAll(newPeers);
        for (Pair<RefCount, Peer> value : newPeers.values()) {
            allPeers.add(value.getRight());
        }

        ArrayList<Peer> needRemoveList = new ArrayList<>();
        GroupInfo oldGroupInfo = groups.get(groupId);
        if (oldGroupInfo != null) {
            for (Peer peer : oldGroupInfo.getServers()) {
                Pair<RefCount, Peer> peerInfo = peers.get(peer.getEndPoint());
                if (peerInfo.getLeft().release()) {
                    needRemoveList.add(peer);
                    peers.remove(peer.getEndPoint());
                }
            }
        }
        this.groups = IntObjMap.copyOnWritePut(groups, groupId, new GroupInfo(groupId, allPeers)).getRight();
        if (needRemoveList.size() > 0) {
            for (Peer peer : needRemoveList) {
                client.removePeer(peer.getEndPoint());
            }
        }
    }

    public synchronized void removeGroup(int groupId) throws NetException {
        Pair<GroupInfo, IntObjMap<GroupInfo>> pair = IntObjMap.copyOnWriteRemove(groups, groupId);
        GroupInfo oldGroupInfo = pair.getLeft();
        this.groups = pair.getRight();

        if (oldGroupInfo != null) {
            for (Peer peer : oldGroupInfo.getServers()) {
                Pair<RefCount, Peer> peerInfo = peers.get(peer.getEndPoint());
                if (peerInfo.getLeft().release()) {
                    peers.remove(peer.getEndPoint());
                }
            }
        }
    }

    public <T> CompletableFuture<ReadFrame<T>> sendRequest(int groupId, WriteFrame request,
                                                           Decoder<T> decoder, DtTime timeout) {
        GroupInfo groupInfo = groups.get(groupId);
        if (groupInfo == null) {
            return DtUtil.failedFuture(new NoSuchGroupException());
        }
        Pair<Peer, CompletableFuture<Peer>> leaderInfo = groupInfo.getLeader();
        CompletableFuture<ReadFrame<T>> finalResult = new CompletableFuture<>();
        CompletableFuture<ReadFrame<T>> result;
        if (leaderInfo != null && leaderInfo.getLeft() != null) {
            Peer leader = leaderInfo.getLeft();
            result = client.sendRequest(leader, request, decoder, timeout);
        } else {
            CompletableFuture<Peer> leaderFuture;
            if (leaderInfo == null) {
                leaderFuture = updateLeaderInfo(groupInfo, timeout);
            } else {
                leaderFuture = leaderInfo.getRight();
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
                    Peer newLeader = parseLeaderFromExtra(ncEx.getRespFrame(), groupInfo);
                    if (newLeader != null) {
                        groupInfo.setLeader(new Pair<>(newLeader, null));
                        if (timeout.isTimeout()) {
                            finalResult.completeExceptionally(new NetTimeoutException("timeout after find new leader"));
                        } else {
                            client.sendRequest(newLeader, request, decoder, timeout).whenComplete((rf2, ex2) -> {
                                if (ex2 == null) {
                                    finalResult.complete(rf2);
                                } else {
                                    finalResult.completeExceptionally(ex2);
                                }
                            });
                        }
                        return;
                    }
                }
                finalResult.completeExceptionally(ex);
            }
        });
        return finalResult;
    }

    private synchronized CompletableFuture<Peer> updateLeaderInfo(GroupInfo groupInfo, DtTime timeout) {
        Pair<Peer, CompletableFuture<Peer>> leaderInfo = groupInfo.getLeader();
        if (leaderInfo == null) {
            CompletableFuture<Peer> f = new CompletableFuture<>();
            leaderInfo = new Pair<>(null, f);
            groupInfo.setLeader(leaderInfo);
            findLeader(f, groupInfo, timeout);
            return f;
        } else {
            return leaderInfo.getRight();
        }
    }

    private void findLeader(CompletableFuture<Peer> f, GroupInfo groupInfo, DtTime timeout) {
        Iterator<Peer> it = groupInfo.getServers().iterator();
        findLeader0(f, groupInfo, timeout, it);
    }

    private void findLeader0(CompletableFuture<Peer> f, GroupInfo groupInfo, DtTime timeout, Iterator<Peer> it) {
        if (timeout.isTimeout()) {
            groupInfo.setLeader(null);
            f.completeExceptionally(new NetTimeoutException("find leader timeout"));
            return;
        }
        if (!it.hasNext()) {
            groupInfo.setLeader(null);
            f.complete(null);
            return;
        }
        Peer p = it.next();
        PbIntWriteFrame req = new PbIntWriteFrame(groupInfo.getGroupId());
        req.setCommand(Commands.RAFT_QUERY_LEADER);
        client.sendRequest(p, req, PbNoCopyDecoder.SIMPLE_STR_DECODER, timeout).whenComplete((rf, ex) -> {
            if (ex != null) {
                log.warn("query leader from {} fail: {}", p.getEndPoint(), ex.toString());
                findLeader0(f, groupInfo, timeout, it);
            } else {
                Peer leader = parseLeader(rf, groupInfo);
                if (leader != null) {
                    groupInfo.setLeader(new Pair<>(leader, null));
                    f.complete(leader);
                } else {
                    findLeader0(f, groupInfo, timeout, it);
                }
            }
        });
    }

    private Peer parseLeaderFromExtra(ReadFrame<?> frame, GroupInfo groupInfo) {
        byte[] bs = frame.getExtra();
        if (bs == null) {
            return null;
        }
        String s = new String(bs, StandardCharsets.UTF_8);
        return parseLeader(groupInfo, s);
    }

    private Peer parseLeader(ReadFrame<String> frame, GroupInfo groupInfo) {
        String s = frame.getBody();
        if (s == null) {
            return null;
        }
        return parseLeader(groupInfo, s);
    }

    private static Peer parseLeader(GroupInfo groupInfo, String s) {
        HostPort hp = NioNet.parseHostPort(s);
        for (Peer p : groupInfo.getServers()) {
            if (p.getEndPoint().equals(hp)) {
                return p;
            }
        }
        log.warn("leader {} not in group {}", s, groupInfo.getGroupId());
        return null;
    }

    @Override
    protected void doStart() {
        client.start();
        client.waitStart();
    }

    @Override
    protected void doStop() {
        client.stop();
    }
}
