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
package com.github.dtprj.dongting.dtkv;

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.GroupInfo;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.raft.RaftClient;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftNode;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class ClientWatchManager {
    private static final DtLog log = DtLogs.getLogger(ClientWatchManager.class);

    public static final byte SEPARATOR = '.';

    private final RaftClient raftClient;
    private final KvClient kvClient;
    private final Supplier<Boolean> stopped;
    private final long heartbeatIntervalMillis;

    private final ReentrantLock lock = new ReentrantLock();
    private final HashMap<Integer, GroupWatch> watches = new HashMap<>();

    private Watch notifyQueueHead;
    private Watch notifyQueueTail;

    private Executor userExecutor;
    private KvListener listener;
    private boolean listenerTaskStart;

    private static class GroupWatch {
        final int groupId;
        final HashMap<String, Watch> watches = new HashMap<>();
        RaftNode server;
        long serversEpoch;
        boolean busy;
        boolean needSync;
        boolean fullSync;
        boolean needCheckServer;

        boolean removed;
        ScheduledFuture<?> scheduledFuture;

        public GroupWatch(int groupId) {
            this.groupId = groupId;
        }
    }

    private static class Watch {
        final String key;

        boolean needRegister = true;
        boolean needRemove;

        long raftIndex;

        WatchEvent event;
        Watch next;

        public Watch(String key) {
            this.key = key;
        }
    }

    ClientWatchManager(KvClient kvClient, Supplier<Boolean> stopped, long heartbeatIntervalMillis) {
        this.raftClient = kvClient.getRaftClient();
        this.kvClient = kvClient;
        this.stopped = stopped;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    private void check(int groupId, String... keys) {
        Objects.requireNonNull(keys);
        for (String key : keys) {
            Objects.requireNonNull(key);
            if (key.isEmpty()) {
                throw new IllegalArgumentException("key must not be empty");
            }
            int len = key.length();
            if (key.charAt(0) == SEPARATOR || key.charAt(len - 1) == SEPARATOR) {
                throw new IllegalArgumentException("invalid key: " + key);
            }
            int lastSep = -1;
            for (int i = 0; i < len; i++) {
                if (key.charAt(i) == SEPARATOR) {
                    if (lastSep == i - 1) {
                        throw new IllegalArgumentException("invalid key: " + key);
                    }
                    lastSep = i;
                }
            }
        }

        if (raftClient.getGroup(groupId) == null) {
            throw new RaftException("group not found: " + groupId);
        }
    }

    public void addWatch(int groupId, String... keys) {
        check(groupId, keys);
        lock.lock();
        try {
            GroupWatch gw = watches.get(groupId);
            if (gw == null) {
                gw = new GroupWatch(groupId);
                watches.put(groupId, gw);
                GroupWatch finalGw = gw;
                Runnable checkTask = () -> {
                    finalGw.needCheckServer = true;
                    this.syncWatches(finalGw);
                };
                gw.scheduledFuture = DtUtil.SCHEDULED_SERVICE.scheduleWithFixedDelay(checkTask,
                        heartbeatIntervalMillis, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
            }
            gw.needSync = true;
            for (String k : keys) {
                Watch w = gw.watches.get(k);
                if (w == null || w.needRemove) {
                    w = new Watch(k);
                    gw.watches.put(k, w);
                }
            }
            syncWatches(gw);
        } finally {
            lock.unlock();
        }
    }

    public void removeWatch(int groupId, String... keys) {
        check(groupId, keys);
        lock.lock();
        try {
            GroupWatch gw = watches.get(groupId);
            if (gw == null) {
                return;
            }
            gw.needSync = true;
            for (String k : keys) {
                Watch w = gw.watches.get(k);
                if (w != null) {
                    w.needRemove = true;
                }
            }
            syncWatches(gw);
        } finally {
            lock.unlock();
        }
    }

    private void syncWatches(GroupWatch gw) {
        if (stopped.get()) {
            return;
        }
        lock.lock();
        try {
            if (gw.busy) {
                return;
            }
            gw.busy = true;

            GroupInfo gi = raftClient.getGroup(gw.groupId);
            if (gi == null) {
                removeGroupWatch(gw);
                return;
            }
            if (gw.server == null || gw.server.peer.getStatus() != PeerStatus.connected) {
                initFindServer(gw, gi);
                return;
            }
            if (gw.needCheckServer) {
                // periodic query (ping), ensure server status and prevent server remove idle client
                sendQueryStatus(gi, gw, gw.server, status -> {
                    if (status == STATUS_OK) {
                        // finished successfully
                        gw.busy = false;
                        gw.needCheckServer = false;
                        if (gw.needSync) {
                            syncWatches(gw);
                        }
                    } else {
                        initFindServer(gw, gi);
                    }
                });
            } else if (gw.needSync) {
                doSync(gw);
            }
        } catch (Exception e) {
            log.error("syncWatches failed, groupId={}", gw.groupId, e);
            gw.busy = false;
        } finally {
            lock.unlock();
        }
    }

    private void doSync(GroupWatch gw) {
        List<byte[]> keys;
        long[] knownRaftIndexes;
        if (gw.fullSync) {
            for (Iterator<Watch> it = gw.watches.values().iterator(); it.hasNext(); ) {
                Watch w = it.next();
                if (w.needRemove) {
                    it.remove();
                } else {
                    w.needRegister = false;
                }
            }
            keys = new ArrayList<>(gw.watches.size());
            knownRaftIndexes = new long[gw.watches.size()];
            int i = 0;
            for (Watch w : gw.watches.values()) {
                keys.add(w.key.getBytes(StandardCharsets.UTF_8));
                knownRaftIndexes[i++] = w.raftIndex;
            }
        } else {
            LinkedList<Watch> list = new LinkedList<>();
            for (Iterator<Watch> it = gw.watches.values().iterator(); it.hasNext(); ) {
                Watch w = it.next();
                if (w.needRemove) {
                    it.remove();
                    list.add(w);
                } else if (w.needRegister) {
                    list.add(w);
                }
            }
            keys = new ArrayList<>(list.size());
            knownRaftIndexes = new long[list.size()];
            int i = 0;
            for (Watch w : list) {
                keys.add(w.key.getBytes(StandardCharsets.UTF_8));
                knownRaftIndexes[i++] = w.raftIndex;
                w.needRegister = false;
                w.needRemove = false;
            }
        }
        gw.needSync = false;
        gw.fullSync = false;
        if (gw.watches.isEmpty()) {
            removeGroupWatch(gw);
        }
        sendSyncReq(gw, keys, knownRaftIndexes);
    }

    private void sendSyncReq(GroupWatch gw, List<byte[]> keys, long[] knownRaftIndexes) {
        RpcCallback<Void> c = (frame, ex) -> {
            if (stopped.get()) {
                return;
            }
            lock.lock();
            try {
                if (gw.removed) {
                    return;
                }
                GroupInfo gi = raftClient.getGroup(gw.groupId);
                if (gi == null) {
                    removeGroupWatch(gw);
                    return;
                }
                if (ex != null) {
                    log.warn("sync watches failed, groupId={}, remote={}, ex={}",
                            gw.groupId, gw.server.peer.getEndPoint(), ex.toString());
                    gw.needSync = true;
                    gw.fullSync = true;
                    gw.server = null;
                    gw.serversEpoch = 0;
                    syncWatches(gw);
                }
            } finally {
                gw.busy = false;
                lock.unlock();
            }
        };
        WatchReq req = new WatchReq(gw.groupId, gw.fullSync, keys, knownRaftIndexes);
        kvClient.sendSyncReq(gw.server, req, c);
    }

    private void removeGroupWatch(GroupWatch gw) {
        log.info("group {} removed", gw.groupId);
        watches.remove(gw.groupId);
        if (gw.scheduledFuture != null) {
            gw.scheduledFuture.cancel(false);
            gw.scheduledFuture = null;
        }
        gw.removed = true;
        gw.busy = false;
        gw.server = null;
        gw.serversEpoch = 0;
    }

    private void initFindServer(GroupWatch gw, GroupInfo gi) {
        gw.server = null;
        gw.serversEpoch = gi.serversEpoch;
        ArrayList<RaftNode> servers = new ArrayList<>(gi.servers);
        Collections.shuffle(servers);
        Iterator<RaftNode> it = servers.iterator();
        findServer(gi, gw, it);
    }


    private void findServer(GroupInfo gi, GroupWatch gw, Iterator<RaftNode> it) {
        while (it.hasNext()) {
            RaftNode node = it.next();
            if (node.peer.getStatus() == PeerStatus.connected) {
                sendQueryStatus(gi, gw, node, status -> {
                    if (status == STATUS_OK) {
                        gw.busy = false;
                        gw.needCheckServer = false;
                        if (gw.needSync) {
                            syncWatches(gw);
                        }
                    } else if (status == STATUS_TRY_NEXT) {
                        findServer(gi, gw, it);
                    } else if (status == STATUS_RESTART_FIND) {
                        initFindServer(gw, gi);
                    }
                });
                return;
            }
        }
        log.error("no server found for group {}", gi.groupId);
        gw.busy = false;
        gw.serversEpoch = 0;
    }

    private static final int STATUS_OK = 0;
    private static final int STATUS_TRY_NEXT = 1;
    private static final int STATUS_RESTART_FIND = 2;

    private void sendQueryStatus(GroupInfo gi, GroupWatch gw, RaftNode n, Consumer<Integer> callback) {
        RpcCallback<KvStatusResp> rpcCallback = (frame, ex) -> {
            if (stopped.get()) {
                return;
            }
            lock.lock();
            try {
                if (gw.removed) {
                    return;
                }
                GroupInfo currentGroupInfo = raftClient.getGroup(gw.groupId);
                if (currentGroupInfo == null) {
                    removeGroupWatch(gw);
                    return;
                }
                if (queryStatusOk(gw.groupId, n, frame, ex)) {
                    if (currentGroupInfo.serversEpoch == gi.serversEpoch || nodeInNewGroupInfo(n, gi)) {
                        gw.server = n;
                        gw.serversEpoch = currentGroupInfo.serversEpoch;
                        callback.accept(STATUS_OK);
                    } else {
                        callback.accept(STATUS_RESTART_FIND);
                    }
                } else {
                    if (currentGroupInfo.serversEpoch == gi.serversEpoch) {
                        callback.accept(STATUS_TRY_NEXT);
                    } else {
                        callback.accept(STATUS_RESTART_FIND);
                    }
                }
            } finally {
                lock.unlock();
            }
        };
        kvClient.sendQueryStatusReq(n, gw.groupId, rpcCallback);
    }

    private boolean queryStatusOk(int groupId, RaftNode n, ReadPacket<KvStatusResp> frame, Throwable ex) {
        if (ex != null) {
            log.warn("query status failed, nodeId={}, groupId={},remote={}, ex={}",
                    n.nodeId, groupId, n.peer.getEndPoint(), ex.toString());
            return false;
        }
        KvStatusResp resp = frame.getBody();
        if (resp == null || resp.raftServerStatus == null) {
            log.warn("query status body is null, nodeId={}, groupId={},remote={}", n.nodeId, groupId);
            return false;
        }
        QueryStatusResp s = resp.raftServerStatus;
        if (s.leaderId <= 0 || s.lastApplyTimeToNowMillis > 15_0000 || s.applyLagMillis > 15_000) {
            log.info("node {} for group {} is not ready, leaderId={}, lastApplyTimeToNowMillis={}, applyLagMillis={}",
                    n.nodeId, groupId, s.leaderId, s.lastApplyTimeToNowMillis, s.applyLagMillis);
            return false;
        }
        return true;
    }

    private boolean nodeInNewGroupInfo(RaftNode n, GroupInfo gi) {
        for (RaftNode node : gi.servers) {
            if (node.nodeId == n.nodeId && node.peer.getEndPoint().equals(n.peer.getEndPoint())) {
                return true;
            }
        }
        return false;
    }

    WritePacket processNotify(WatchNotifyReq req, SocketAddress remote) {
        lock.lock();
        try {
            GroupWatch watch = watches.get(req.groupId);
            if (watch == null) {
                log.warn("watch group not found, groupId={}, server={}", req.groupId, remote);
                EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
                p.setBizCode(KvCodes.CODE_REMOVE_ALL_WATCH);
                return p;
            }
            if (req.notifyList == null || req.notifyList.isEmpty()) {
                return new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            }
            int[] results = new int[req.notifyList.size()];
            int i = 0;
            for (WatchNotify n : req.notifyList) {
                String k = new String(n.key, StandardCharsets.UTF_8);
                Watch w = watch.watches.get(k);
                if (w == null || w.needRemove) {
                    results[i] = KvCodes.CODE_REMOVE_WATCH;
                } else {
                    if (w.raftIndex < n.raftIndex) {
                        w.raftIndex = n.raftIndex;
                        WatchEvent e = new WatchEvent(watch.groupId, n.raftIndex, n.state, k, n.value);
                        addOrUpdateToNotifyQueue(w, e);
                    }
                    results[i] = KvCodes.CODE_SUCCESS;
                }
                i++;
            }
            fireListenerEventInLock();
            WatchNotifyResp resp = new WatchNotifyResp(results);
            return new EncodableBodyWritePacket(resp);
        } finally {
            lock.unlock();
        }
    }

    private void fireListenerEventInLock() {
        if (stopped.get()) {
            return;
        }
        if (listener != null && !listenerTaskStart && notifyQueueHead != null) {
            listenerTaskStart = true;

            Executor executor;
            if (userExecutor != null) {
                executor = userExecutor;
            } else {
                executor = raftClient.getNioClient().getBizExecutor();
            }
            if (executor == null) {
                log.warn("no executor for watch listener, create single thread executor");
                userExecutor = Executors.newSingleThreadExecutor();
                executor = userExecutor;
            }
            try {
                executor.execute(this::runListenerTask);
            } catch (Throwable e) {
                log.error("", e);
                listenerTaskStart = false;
            }
        }
    }

    private void runListenerTask() {
        if (stopped.get()) {
            return;
        }
        KvListener listener;
        WatchEvent e;
        lock.lock();
        try {
            listener = this.listener;
            e = takeEventInLock();
        } finally {
            lock.unlock();
        }
        try {
            if (listener != null && e != null) {
                listener.onUpdate(e);
            }
        } catch (Throwable ex) {
            log.error("watch listener task failed", ex);
        }
        lock.lock();
        try {
            listenerTaskStart = false;
            fireListenerEventInLock();
        } finally {
            lock.unlock();
        }
    }

    private void addOrUpdateToNotifyQueue(Watch w, WatchEvent e) {
        if (w.event != null) {
            w.event = e;
            return;
        }
        w.event = e;
        if (notifyQueueHead == null) {
            notifyQueueHead = w;
        } else {
            notifyQueueTail.next = w;
        }
        notifyQueueTail = w;
    }

    /**
     * manually take an event from notify queue.
     */
    public WatchEvent takeEvent() {
        lock.lock();
        try {
            return takeEventInLock();
        } finally {
            lock.unlock();
        }
    }

    private WatchEvent takeEventInLock() {
        Watch w = notifyQueueHead;
        while (w != null && w.needRemove) {
            w = w.next;
        }
        if (w == null) {
            notifyQueueHead = null;
            notifyQueueTail = null;
            return null;
        }
        WatchEvent e = w.event;
        w.event = null;
        notifyQueueHead = w.next;
        if (notifyQueueHead == null) {
            notifyQueueTail = null;
        }
        return e;
    }

    /**
     * Set listener and user executor for watch events.
     * The listener callback will be executed in a globally serialized manner.
     * @param listener the use listener
     * @param userExecutor if null use NioClient's bizExecutor as default
     */
    public void setListener(KvListener listener, Executor userExecutor) {
        lock.lock();
        this.listener = listener;
        this.userExecutor = userExecutor;
        lock.unlock();
    }

    /**
     * Set listener and user executor for watch events.
     * The listener callback will be executed in a globally serialized manner, in NioClient's bizExecutor.
     * @param listener the use listener
     */
    public void setListener(KvListener listener) {
        setListener(listener, null);
    }

}
