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

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.RefCount;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NetException;
import com.github.dtprj.dongting.net.NetTimeoutException;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.Peer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author huangli
 */
public class RaftClient extends AbstractLifeCircle {
    private final NioClient client;
    private final Map<HostPort, Pair<RefCount, Peer>> peers = new HashMap<>();
    private volatile IntObjMap<GroupInfo> groups = new IntObjMap<>();

    public RaftClient() {
        NioClientConfig nioClientConfig = new NioClientConfig();
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
        HashMap<HostPort, Pair<RefCount, Peer>> newPeers = new HashMap<>(peers);
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
