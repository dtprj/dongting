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

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.PbIntWritePacket;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
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

    private final RaftClient raftClient;
    private final Supplier<Boolean> stopped;
    private final long heartbeatIntervalMillis;

    private final ReentrantLock lock = new ReentrantLock();
    private final HashMap<Integer, GroupWatches> watches = new HashMap<>();

    private KeyWatch notifyQueueHead;
    private KeyWatch notifyQueueTail;

    private Executor userExecutor;
    private KvListener listener;
    private boolean listenerTaskStart;

    private static class GroupWatches {
        final int groupId;
        final HashMap<ByteArray, KeyWatch> watches = new HashMap<>();
        RaftNode server;
        long serversEpoch;
        boolean busy;
        boolean needSync;
        boolean fullSync;
        boolean needCheckServer;

        boolean removed;
        ScheduledFuture<?> scheduledFuture;

        public GroupWatches(int groupId) {
            this.groupId = groupId;
        }
    }

    static class KeyWatch {
        private final ByteArray key;
        private final GroupWatches gw;

        private boolean needRegister = true;
        private boolean needRemove;

        private long raftIndex;

        private WatchEvent event;
        private KeyWatch next;

        private KeyWatch(ByteArray key, GroupWatches gw) {
            this.key = key;
            this.gw = gw;
        }
    }

    protected ClientWatchManager(KvClient kvClient, Supplier<Boolean> stopped, long heartbeatIntervalMillis) {
        this.raftClient = kvClient.getRaftClient();
        this.stopped = stopped;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    private void check(int groupId, byte[]... keys) {
        Objects.requireNonNull(keys);
        for (byte[] key : keys) {
            Objects.requireNonNull(key);
            int c = KvClient.checkKey(key, KvClient.MAX_KEY_SIZE, false, true);
            if (c != KvCodes.SUCCESS) {
                throw new IllegalArgumentException(KvCodes.toStr(c));
            }
        }

        if (raftClient.getGroup(groupId) == null) {
            throw new RaftException("group not found: " + groupId);
        }
    }

    public void addWatch(int groupId, byte[]... keys) {
        check(groupId, keys);
        lock.lock();
        try {
            GroupWatches gw = watches.get(groupId);
            if (gw == null) {
                gw = new GroupWatches(groupId);
                watches.put(groupId, gw);
                GroupWatches finalGw = gw;
                Runnable checkTask = () -> {
                    lock.lock();
                    try {
                        finalGw.needCheckServer = true;
                        syncGroupInLock(finalGw);
                    } finally {
                        lock.unlock();
                    }
                };
                gw.scheduledFuture = DtUtil.SCHEDULED_SERVICE.scheduleWithFixedDelay(checkTask,
                        heartbeatIntervalMillis, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
            }
            for (byte[] k : keys) {
                ByteArray key = new ByteArray(k);
                KeyWatch w = gw.watches.get(key);
                if (w == null || w.needRemove) {
                    w = new KeyWatch(key, gw);
                    gw.watches.put(key, w);
                    gw.needSync = true;
                }
            }
            if (gw.needSync) {
                syncGroupInLock(gw);
            }
        } finally {
            lock.unlock();
        }
    }

    public void removeWatch(int groupId, byte[]... keys) {
        check(groupId, keys);
        lock.lock();
        try {
            GroupWatches gw = watches.get(groupId);
            if (gw == null || gw.removed) {
                return;
            }
            for (byte[] k : keys) {
                ByteArray key = new ByteArray(k);
                KeyWatch w = gw.watches.get(key);
                if (w != null) {
                    w.needRemove = true;
                    gw.needSync = true;
                }
            }
            if (gw.needSync) {
                syncGroupInLock(gw);
            }
        } finally {
            lock.unlock();
        }
    }

    private void syncGroupInLock(GroupWatches gw) {
        try {
            if (stopped.get()) {
                return;
            }
            if (gw.busy || gw.removed) {
                return;
            }
            gw.busy = true;

            GroupInfo gi = raftClient.getGroup(gw.groupId);
            if (gi == null) {
                removeGroupWatches(gw);
                return;
            }
            if (gw.server == null || gw.server.peer.status != PeerStatus.connected) {
                if (isGroupWatchesValid(gw)) {
                    initFindServer(gw, gi);
                } else {
                    // not send sync request
                    removeGroupWatches(gw);
                }
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
                            syncGroupInLock(gw);
                        }
                    } else {
                        initFindServer(gw, gi);
                    }
                });
            } else if (gw.needSync) {
                syncGroupInLock0(gw);
            }
        } catch (Throwable e) {
            log.error("sync watches failed, groupId={}", gw.groupId, e);
            gw.busy = false;
        }
    }

    private boolean isGroupWatchesValid(GroupWatches gw) {
        for (KeyWatch kw : gw.watches.values()) {
            if (!kw.needRemove) {
                return true;
            }
        }
        return false;
    }

    private void syncGroupInLock0(GroupWatches gw) {
        List<ByteArray> keys;
        long[] knownRaftIndexes;
        if (gw.fullSync) {
            for (Iterator<KeyWatch> it = gw.watches.values().iterator(); it.hasNext(); ) {
                KeyWatch w = it.next();
                if (w.needRemove) {
                    it.remove();
                } else {
                    w.needRegister = false;
                }
            }
            keys = new ArrayList<>(gw.watches.size());
            knownRaftIndexes = new long[gw.watches.size()];
            int i = 0;
            for (KeyWatch w : gw.watches.values()) {
                keys.add(w.key);
                knownRaftIndexes[i++] = w.raftIndex;
            }
        } else {
            LinkedList<KeyWatch> list = new LinkedList<>();
            for (Iterator<KeyWatch> it = gw.watches.values().iterator(); it.hasNext(); ) {
                KeyWatch w = it.next();
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
            for (KeyWatch w : list) {
                keys.add(w.key);
                knownRaftIndexes[i++] = w.raftIndex;
                w.needRegister = false;
                w.needRemove = false;
            }
        }
        gw.needSync = false;
        gw.fullSync = false;
        if (gw.watches.isEmpty()) {
            removeGroupWatches(gw);
        }
        sendSyncReq(gw, keys, knownRaftIndexes);
    }

    private void sendSyncReq(GroupWatches gw, List<ByteArray> keys, long[] knownRaftIndexes) {
        RpcCallback<Void> c = (frame, ex) -> {
            if (stopped.get()) {
                return;
            }
            lock.lock();
            try {
                GroupInfo gi = raftClient.getGroup(gw.groupId);
                if (gi == null) {
                    removeGroupWatches(gw);
                    return;
                }
                if (ex != null) {
                    log.warn("sync watches failed, groupId={}, remote={}, ex={}",
                            gw.groupId, gw.server.peer.endPoint, ex.toString());
                    gw.needSync = true;
                    gw.fullSync = true;
                    gw.server = null;
                    gw.serversEpoch = 0;
                    syncGroupInLock(gw);
                }
            } catch (Throwable e) {
                log.error("", e);
            } finally {
                gw.busy = false;
                lock.unlock();
            }
        };
        WatchReq req = new WatchReq(gw.groupId, gw.fullSync, keys, knownRaftIndexes);
        sendSyncReq(gw.server, req, c);
    }

    protected void sendSyncReq(RaftNode n, WatchReq req, RpcCallback<Void> c) {
        EncodableBodyWritePacket packet = new EncodableBodyWritePacket(req);
        packet.setCommand(Commands.DTKV_SYNC_WATCH);
        raftClient.getNioClient().sendRequest(n.peer, packet,
                DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR, raftClient.createDefaultTimeout(), c);
    }

    private void removeGroupWatches(GroupWatches gw) {
        log.info("group {} removed", gw.groupId);
        watches.remove(gw.groupId);
        if (gw.scheduledFuture != null) {
            gw.scheduledFuture.cancel(false);
            gw.scheduledFuture = null;
        }
        gw.removed = true;
        gw.busy = false;
        gw.serversEpoch = 0;
    }

    private void initFindServer(GroupWatches gw, GroupInfo gi) {
        gw.server = null;
        gw.serversEpoch = gi.serversEpoch;
        ArrayList<RaftNode> servers = new ArrayList<>(gi.servers);
        findServer(gi, gw, servers);
    }

    private void findServer(GroupInfo gi, GroupWatches gw, List<RaftNode> list) {
        if (list.isEmpty()) {
            log.error("no server found for group {}", gi.groupId);
            gw.busy = false;
            gw.serversEpoch = 0;
        } else {
            list.sort(Comparator.comparingInt(o -> o.peer.connectRetryCount));
            RaftNode node = list.remove(0);
            findServer(gi, gw, list, node);
        }
    }

    private void findServer(GroupInfo gi, GroupWatches gw, List<RaftNode> list, RaftNode node) {
        if (node.peer.status == PeerStatus.connected) {
            sendQueryStatus(gi, gw, node, status -> {
                if (status == STATUS_OK) {
                    gw.busy = false;
                    gw.needCheckServer = false;
                    if (gw.needSync) {
                        syncGroupInLock(gw);
                    }
                } else if (status == STATUS_TRY_NEXT) {
                    findServer(gi, gw, list);
                } else if (status == STATUS_RESTART_FIND) {
                    initFindServer(gw, gi);
                }
            });
        } else {
            raftClient.getNioClient().connect(node.peer).whenComplete((v, ex) -> {
                if (ex != null) {
                    // try next
                    findServer(gi, gw, list);
                } else {
                    findServer(gi, gw, list, node);
                }
            });
        }
    }

    private static final int STATUS_OK = 0;
    private static final int STATUS_TRY_NEXT = 1;
    private static final int STATUS_RESTART_FIND = 2;

    private void sendQueryStatus(GroupInfo gi, GroupWatches gw, RaftNode n, Consumer<Integer> callback) {
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
                    removeGroupWatches(gw);
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
            } catch (Throwable e) {
                log.error("", e);
                gw.busy = false;
            } finally {
                lock.unlock();
            }
        };
        sendQueryStatusReq(n, gw.groupId, rpcCallback);
    }

    protected void sendQueryStatusReq(RaftNode n, int groupId, RpcCallback<KvStatusResp> rpcCallback) {
        PbIntWritePacket p = new PbIntWritePacket(Commands.DTKV_QUERY_STATUS, groupId);
        DecoderCallbackCreator<KvStatusResp> d = ctx -> ctx.toDecoderCallback(new KvStatusResp());
        raftClient.getNioClient().sendRequest(n.peer, p, d, raftClient.createDefaultTimeout(), rpcCallback);
    }

    private boolean queryStatusOk(int groupId, RaftNode n, ReadPacket<KvStatusResp> frame, Throwable ex) {
        if (ex != null) {
            log.warn("query status failed, nodeId={}, groupId={},remote={}, ex={}",
                    n.nodeId, groupId, n.peer.endPoint, ex.toString());
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
            if (node.nodeId == n.nodeId && node.peer.endPoint.equals(n.peer.endPoint)) {
                return true;
            }
        }
        return false;
    }

    protected WritePacket processNotify(WatchNotifyReq req, SocketAddress remote) {
        lock.lock();
        try {
            GroupWatches watch = watches.get(req.groupId);
            if (watch == null) {
                log.warn("watch group not found, groupId={}, server={}", req.groupId, remote);
                EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
                p.setBizCode(KvCodes.REMOVE_ALL_WATCH);
                return p;
            }
            if (req.notifyList == null || req.notifyList.isEmpty()) {
                return new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            }
            int[] results = new int[req.notifyList.size()];
            int i = 0;
            for (WatchNotify n : req.notifyList) {
                ByteArray k = new ByteArray(n.key);
                KeyWatch w = watch.watches.get(k);
                if (w == null || w.needRemove) {
                    results[i] = KvCodes.REMOVE_WATCH;
                } else {
                    if (w.raftIndex < n.raftIndex) {
                        w.raftIndex = n.raftIndex;
                        WatchEvent e = new WatchEvent(watch.groupId, n.raftIndex, n.state, n.key, n.value);
                        addOrUpdateToNotifyQueue(w, e);
                    }
                    results[i] = KvCodes.SUCCESS;
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
            try {
                userExecutor.execute(this::runListenerTask);
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
        WatchEvent e = null;
        lock.lock();
        try {
            listener = this.listener;
            if (listener != null) {
                e = takeEventInLock();
            }
        } finally {
            lock.unlock();
        }
        try {
            if (e != null) {
                if (raftClient.getGroup(e.groupId) != null) {
                    listener.onUpdate(e);
                }
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

    private void addOrUpdateToNotifyQueue(KeyWatch w, WatchEvent e) {
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
        KeyWatch w = notifyQueueHead;
        while (w != null && w.needRemove && w.gw.removed) {
            w.event = null;
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
     */
    public void setListener(KvListener listener, Executor userExecutor) {
        Objects.requireNonNull(listener);
        Objects.requireNonNull(userExecutor);
        lock.lock();
        this.listener = listener;
        this.userExecutor = userExecutor;
        lock.unlock();
    }

    public void removeListener() {
        lock.lock();
        this.listener = null;
        this.userExecutor = null;
        lock.unlock();
    }

}
