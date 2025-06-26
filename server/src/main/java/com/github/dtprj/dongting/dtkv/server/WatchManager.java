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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.WatchEvent;
import com.github.dtprj.dongting.dtkv.WatchNotify;
import com.github.dtprj.dongting.dtkv.WatchNotifyReq;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.DtChannel;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.raft.RaftException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
abstract class WatchManager {
    private static final DtLog log = DtLogs.getLogger(WatchManager.class);
    private final IdentityHashMap<DtChannel, ChannelInfo> channelInfoMap = new IdentityHashMap<>();
    private final LinkedHashSet<ChannelInfo> needNotifyChannels = new LinkedHashSet<>();
    private final PriorityQueue<ChannelInfo> retryQueue = new PriorityQueue<>();
    ChannelInfo activeQueueHead;
    ChannelInfo activeQueueTail;

    private final LinkedHashSet<WatchHolder> needDispatch = new LinkedHashSet<>();

    private final int groupId;
    private final Timestamp ts;
    private final KvConfig config;
    private final long[] retryIntervalNanos;
    private int epoch;

    private final ArrayList<Pair<ChannelWatch, WatchNotify>> tempList = new ArrayList<>(64);

    WatchManager(int groupId, Timestamp ts, KvConfig config) {
        this(groupId, ts, config, new long[]{1000, 10_000, 30_000, 60_000});
    }

    WatchManager(int groupId, Timestamp ts, KvConfig config, long[] retryIntervalMillis) {
        this.groupId = groupId;
        this.ts = ts;
        this.config = config;
        this.retryIntervalNanos = new long[retryIntervalMillis.length];
        for (int i = 0; i < retryIntervalMillis.length; i++) {
            this.retryIntervalNanos[i] = TimeUnit.MILLISECONDS.toNanos(retryIntervalMillis[i]);
        }
    }

    public void reset() {
        epoch++;
        needNotifyChannels.clear();
        channelInfoMap.clear();
        retryQueue.clear();
        activeQueueHead = null;
        activeQueueTail = null;
    }

    void addOrUpdateActiveQueue(ChannelInfo ci) {
        // is current tail
        if (activeQueueTail == ci) {
            return;
        }

        // already in the queue
        if (ci.next != null) {
            if (ci.prev != null) {
                ci.prev.next = ci.next;
            } else {
                activeQueueHead = ci.next;
            }
            ci.next.prev = ci.prev;
        }

        // add to tail
        if (activeQueueHead == null) {
            activeQueueHead = ci;
        } else {
            activeQueueTail.next = ci;
            ci.prev = activeQueueTail;
        }
        activeQueueTail = ci;
        ci.next = null;
    }

    void removeFromActiveQueue(ChannelInfo ci) {
        if (ci.prev == null && ci.next == null && activeQueueHead != ci) {
            // not in the queue
            return;
        }
        if (ci.prev != null) {
            ci.prev.next = ci.next;
        } else {
            activeQueueHead = ci.next;
        }
        if (ci.next != null) {
            ci.next.prev = ci.prev;
        } else {
            activeQueueTail = ci.prev;
        }
        ci.prev = null;
        ci.next = null;
    }

    private static ChannelWatch createWatch(KvImpl kv, ByteArray key, ChannelInfo ci, long notifiedIndex) {
        KvNodeHolder nodeHolder = kv.map.get(key);
        WatchHolder wh = ensureWatchHolder(kv, key, nodeHolder);
        ChannelWatch w = new ChannelWatch(wh, ci, notifiedIndex);
        wh.watches.add(w);
        return w;
    }

    private static WatchHolder ensureWatchHolder(KvImpl kv, ByteArray key, KvNodeHolder nodeHolder) {
        KvNodeHolder parent = null;
        if (nodeHolder != null) {
            if (!nodeHolder.latest.removed) {
                if (nodeHolder.watchHolder == null) {
                    nodeHolder.watchHolder = new WatchHolder(nodeHolder.key, nodeHolder, null);
                }
                // mount to node with same key
                return nodeHolder.watchHolder;
            }
            parent = nodeHolder.parent;
        }
        // mount to parent dir
        ByteArray parentKey;
        if (parent == null) {
            parentKey = kv.parentKey(key);
            parent = kv.map.get(parentKey);
        } else {
            parentKey = parent.key;
        }
        WatchHolder parentWatchHolder = ensureWatchHolder(kv, parentKey, parent);
        WatchHolder watchHolder = parentWatchHolder.getChild(key);
        if (watchHolder == null) {
            watchHolder = new WatchHolder(key, null, parentWatchHolder);
            parentWatchHolder.addChild(key, watchHolder);
        }
        return watchHolder;
    }

    public void afterUpdate(KvNodeHolder h) {
        WatchHolder wh = h.watchHolder;
        if (wh == null) {
            return;
        }
        if (wh.waitingDispatch) {
            return;
        }
        wh.waitingDispatch = true;
        needDispatch.add(wh);
    }

    public void mountWatchToParent(KvNodeHolder h) {
        WatchHolder wh = h.watchHolder;
        if (wh != null) {
            wh.lastRemoveIndex = h.updateIndex;
            KvNodeHolder parent = h.parent;
            if (parent.watchHolder == null) {
                parent.watchHolder = new WatchHolder(parent.key, parent, null);
            }
            parent.watchHolder.addChild(h.key, wh);
            wh.parentWatchHolder = parent.watchHolder;
            wh.nodeHolder = null;
        }
    }

    public void mountWatchToChild(KvNodeHolder h) {
        WatchHolder parentWh = h.parent.watchHolder;
        if (parentWh != null) {
            HashMap<ByteArray, WatchHolder> children = parentWh.children;
            if (children != null) {
                WatchHolder wh = children.remove(h.key);
                if (wh != null) {
                    h.watchHolder = wh;
                    wh.key = h.key;
                    wh.nodeHolder = h;
                    wh.parentWatchHolder = null;
                }
            }
        }
    }

    private void removeWatchFromKvTree(ChannelWatch w) {
        if (w.removed) {
            return;
        }
        w.removed = true;
        WatchHolder h = w.watchHolder;
        h.watches.remove(w);
        while (h.isNoUse()) {
            // this key has no watches, remove watch holder from tree
            if (h.nodeHolder != null) {
                h.nodeHolder.watchHolder = null;
                break;
            } else {
                h.parentWatchHolder.removeChild(h.key);
                h = h.parentWatchHolder;
            }
        }
    }

    public void removeByChannel(DtChannel channel) {
        ChannelInfo ci = channelInfoMap.remove(channel);
        if (ci != null && !ci.remove) {
            ci.remove = true;

            needNotifyChannels.remove(ci);
            retryQueue.remove(ci);
            removeFromActiveQueue(ci);

            for (Iterator<ChannelWatch> it = ci.watches.values().iterator(); it.hasNext(); ) {
                ChannelWatch w = it.next();
                it.remove();
                removeWatchFromKvTree(w);
            }
        }
    }

    public boolean dispatch() {
        boolean result = true;
        int dispatchBatchSize = config.watchMaxBatchSize;
        try {
            int count = 0;
            if (!needDispatch.isEmpty()) {
                Iterator<WatchHolder> it = needDispatch.iterator();
                while (it.hasNext()) {
                    WatchHolder wh = it.next();
                    if (++count > dispatchBatchSize) {
                        result = false;
                        break;
                    }
                    for (ChannelWatch w : wh.watches) {
                        if (w.removed || w.pending) {
                            continue;
                        }
                        ChannelInfo ci = w.channelInfo;
                        ci.addNeedNotify(w);
                        if (ci.failCount == 0 && !ci.pending) {
                            needNotifyChannels.add(ci);
                        }
                    }
                    wh.waitingDispatch = false;
                    it.remove();
                }
            }

            count = 0;
            if (!needNotifyChannels.isEmpty()) {
                Iterator<ChannelInfo> it = needNotifyChannels.iterator();
                while (it.hasNext()) {
                    ChannelInfo ci = it.next();
                    if (ci.failCount == 0) {
                        if (++count > dispatchBatchSize) {
                            result = false;
                            break;
                        }
                        pushNotify(ci);
                    }
                    it.remove();
                }
            }

            count = 0;
            ChannelInfo ci = retryQueue.peek();
            while (ci != null && ci.retryNanos - ts.nanoTime <= 0) {
                if (++count > dispatchBatchSize) {
                    result = false;
                    break;
                }
                retryQueue.poll();
                pushNotify(ci);
                ci = retryQueue.peek();
            }
        } catch (Throwable e) {
            log.error("", e);
        }
        return result;
    }

    private void pushNotify(ChannelInfo ci) {
        if (!ci.channel.getChannel().isOpen()) {
            removeByChannel(ci.channel);
        }
        if (ci.remove) {
            return;
        }
        if (ci.needNotify == null || ci.needNotify.isEmpty()) {
            return;
        }
        Iterator<ChannelWatch> it = ci.needNotify.iterator();
        int bytes = 0;
        ArrayList<Pair<ChannelWatch, WatchNotify>> list = tempList;
        try {
            while (it.hasNext()) {
                ChannelWatch w = it.next();
                it.remove();
                if (w.removed || w.pending) {
                    continue;
                }
                WatchNotify wn = createNotify(w);
                if (wn != null) {
                    list.add(new Pair<>(w, wn));
                    w.pending = true;
                    bytes += wn.key.length + (wn.value == null ? 0 : wn.value.length);
                    if (bytes > config.watchMaxReqBytes) {
                        break;
                    }
                }
            }
            if (list.isEmpty()) {
                ci.pending = false;
            } else {
                ci.pending = true;
                ci.lastNotifyNanos = ts.nanoTime;
                ArrayList<ChannelWatch> watchList = new ArrayList<>(list.size());
                ArrayList<WatchNotify> notifyList = new ArrayList<>(list.size());
                for (Pair<ChannelWatch, WatchNotify> p : list) {
                    watchList.add(p.getLeft());
                    notifyList.add(p.getRight());
                }
                WatchNotifyReq req = new WatchNotifyReq(groupId, notifyList);
                sendRequest(ci, req, watchList, epoch, it.hasNext());
            }
        } catch (Error | RuntimeException e) {
            ci.pending = false;
            reAddToNeedNotifyIfNeeded(ci);
            throw e;
        } finally {
            list.clear();
        }
    }

    protected abstract void sendRequest(ChannelInfo ci, WatchNotifyReq req, ArrayList<ChannelWatch> watchList,
                                        int requestEpoch, boolean fireNext);

    private WatchNotify createNotify(ChannelWatch w) {
        KvNodeHolder node = w.watchHolder.nodeHolder;
        if (node != null) {
            long updateIndex = node.latest.updateIndex;
            if (w.notifiedIndex >= updateIndex) {
                return null;
            }
            byte[] key = node.key.getData();
            w.notifiedIndexPending = updateIndex;
            // assert note.latest.removed == false
            if (node.latest.isDir) {
                return new WatchNotify(updateIndex, WatchEvent.STATE_DIRECTORY_EXISTS, key, null);
            } else {
                return new WatchNotify(updateIndex, WatchEvent.STATE_VALUE_EXISTS, key, node.latest.data);
            }
        } else {
            long lastRemoveIndex = w.watchHolder.lastRemoveIndex;
            if (w.notifiedIndex >= lastRemoveIndex) {
                return null;
            } else {
                w.notifiedIndexPending = lastRemoveIndex;
                return new WatchNotify(lastRemoveIndex, WatchEvent.STATE_NOT_EXISTS,
                        w.watchHolder.key.getData(), null);
            }
        }
    }

    public void processNotifyResult(ChannelInfo ci, ArrayList<ChannelWatch> watches,
                                    ReadPacket<WatchNotifyRespCallback> result,
                                    Throwable ex, int requestEpoch, boolean fireNext) {
        try {
            for (int size = watches.size(), i = 0; i < size; i++) {
                ChannelWatch w = watches.get(i);
                w.pending = false;
            }
            ci.pending = false;

            if (epoch != requestEpoch) {
                return;
            }
            if (ex != null) {
                log.warn("notify failed. remote={}, ex={}", ci.channel.getRemoteAddr(), ex);
                if (ex instanceof NetCodeException) {
                    NetCodeException nce = (NetCodeException) ex;
                    if (nce.getCode() == CmdCodes.CLIENT_ERROR || nce.getCode() == CmdCodes.STOPPING
                            || nce.getCode() == CmdCodes.COMMAND_NOT_SUPPORT) {
                        removeByChannel(ci.channel);
                        return;
                    }
                }
                retryByChannel(ci, watches);
            } else if (result.getBizCode() == KvCodes.CODE_SUCCESS) {
                WatchNotifyRespCallback callback = result.getBody();
                if (callback.results.length != watches.size()) {
                    log.error("response results size not match, expect {}, but got {}",
                            watches.size(), callback.results.length);
                    removeByChannel(ci.channel);
                    return;
                }

                boolean hasFailCode = false;
                for (int size = watches.size(), i = 0; i < size; i++) {
                    int bizCode = callback.results[i];
                    ChannelWatch w = watches.get(i);
                    if (bizCode == KvCodes.CODE_REMOVE_WATCH) {
                        ci.watches.remove(w.watchHolder.key);
                        removeWatchFromKvTree(w);
                    } else {
                        if (bizCode != KvCodes.CODE_SUCCESS) {
                            hasFailCode = true;
                            log.error("notify failed. remote={}, bizCode={}", ci.channel.getRemoteAddr(), bizCode);
                        } else {
                            w.notifiedIndex = w.notifiedIndexPending;
                            ci.addNeedNotify(w); // remove in pushNotify(ChannelInfo) method
                        }
                    }
                }
                if (hasFailCode) {
                    retryByChannel(ci, watches);
                    return;
                }
                ci.failCount = 0;
                ci.lastActiveNanos = ts.nanoTime;
                if (ci.watches.isEmpty()) {
                    removeByChannel(ci.channel);
                } else if (fireNext) {
                    pushNotify(ci);
                } else {
                    reAddToNeedNotifyIfNeeded(ci);
                }
            } else if (result.getBizCode() == KvCodes.CODE_REMOVE_ALL_WATCH) {
                removeByChannel(ci.channel);
            } else {
                log.error("notify failed. remote={}, bizCode={}", ci.channel.getRemoteAddr(), result.getBizCode());
                retryByChannel(ci, watches);
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private void reAddToNeedNotifyIfNeeded(ChannelInfo ci) {
        if (ci.needNotify != null && !ci.needNotify.isEmpty()) {
            needNotifyChannels.add(ci);
        }
    }

    private void retryByChannel(ChannelInfo ci, ArrayList<ChannelWatch> watches) {
        ci.failCount++;
        int idx = Math.min(ci.failCount - 1, retryIntervalNanos.length - 1);
        ci.retryNanos = ts.nanoTime + retryIntervalNanos[idx];
        retryQueue.add(ci);
        for (int size = watches.size(), i = 0; i < size; i++) {
            ci.addNeedNotify(watches.get(i));
        }
    }

    public void cleanTimeoutChannel(long timeoutNanos) {
        try {
            while (activeQueueHead != null) {
                if (ts.nanoTime - activeQueueHead.lastActiveNanos > timeoutNanos) {
                    removeByChannel(activeQueueHead.channel);
                } else {
                    return;
                }
            }
        } catch (Throwable e) {
            log.error("", e);
        }
    }

    public void sync(KvImpl kv, DtChannel channel, boolean syncAll, ByteArray[] keys, long[] knownRaftIndexes) {
        if (syncAll && (keys == null || keys.length == 0)) {
            removeByChannel(channel);
            return;
        }
        ChannelInfo ci = channelInfoMap.get(channel);
        if (ci == null) {
            ci = new ChannelInfo(channel, ts.nanoTime);
            channelInfoMap.put(channel, ci);
        }
        addOrUpdateActiveQueue(ci);

        if (syncAll) {
            for (ChannelWatch cw : ci.watches.values()) {
                cw.needRemoveAfterSyncAll = true;
            }
        }

        for (int i = 0; i < keys.length; i++) {
            ByteArray key = keys[i];
            long knownRaftIndex = knownRaftIndexes[i];
            if (knownRaftIndex >= 0) {
                ChannelWatch w = ci.watches.get(key);
                if (w == null) {
                    w = createWatch(kv, key, ci, knownRaftIndex);
                    ci.watches.put(w.watchHolder.key, w);
                } else {
                    w.notifiedIndex = Math.max(w.notifiedIndex, knownRaftIndex);
                    w.needRemoveAfterSyncAll = false;
                }
                if (w.removed || w.pending) {
                    continue;
                }
                ci.addNeedNotify(w);
                if (ci.failCount == 0 && !ci.pending) {
                    needNotifyChannels.add(ci);
                }
            } else {
                ChannelWatch w = ci.watches.remove(key);
                if (w != null) {
                    removeWatchFromKvTree(w);
                }
            }
        }

        if (syncAll) {
            for (Iterator<ChannelWatch> it = ci.watches.values().iterator(); it.hasNext(); ) {
                ChannelWatch cw = it.next();
                if (cw.needRemoveAfterSyncAll) {
                    it.remove();
                    removeWatchFromKvTree(cw);
                }
            }
        }

        if (ci.watches.isEmpty()) {
            // this channel has no watches, remove channel info
            removeByChannel(ci.channel);
        }
    }

    public int updateWatchStatus(DtChannel dtc) {
        ChannelInfo ci = channelInfoMap.get(dtc);
        if (ci == null) {
            return 0;
        } else {
            if (ts.nanoTime - ci.lastNotifyNanos > 1_000_000_000L) {
                ci.needNotify = null;
            }
            ci.lastActiveNanos = ts.nanoTime;
            addOrUpdateActiveQueue(ci);
            return ci.watches.size();
        }
    }
}

final class ChannelInfo implements Comparable<ChannelInfo> {
    final DtChannel channel;
    final HashMap<ByteArray, ChannelWatch> watches = new HashMap<>(4);

    ChannelInfo prev;
    ChannelInfo next;

    boolean pending;
    long lastNotifyNanos;
    long lastActiveNanos;

    LinkedHashSet<ChannelWatch> needNotify;

    long retryNanos;
    int failCount;

    boolean remove;

    ChannelInfo(DtChannel channel, long now) {
        this.channel = channel;
        this.lastActiveNanos = now;
        this.lastNotifyNanos = now;
    }

    @Override
    public int compareTo(ChannelInfo o) {
        long diff = retryNanos - o.retryNanos;
        return diff < 0 ? -1 : (diff > 0 ? 1 : 0);
    }

    public void addNeedNotify(ChannelWatch w) {
        if (needNotify == null) {
            needNotify = new LinkedHashSet<>();
        }
        needNotify.add(w);
    }

}

final class ChannelWatch {
    final WatchHolder watchHolder;
    final ChannelInfo channelInfo;

    long notifiedIndex;
    long notifiedIndexPending;

    boolean pending;
    boolean removed;
    boolean needRemoveAfterSyncAll;

    ChannelWatch(WatchHolder watchHolder, ChannelInfo channelInfo, long notifiedIndex) {
        this.watchHolder = watchHolder;
        this.channelInfo = channelInfo;
        this.notifiedIndex = notifiedIndex;
        this.notifiedIndexPending = notifiedIndex;
    }
}

final class WatchHolder {
    final HashSet<ChannelWatch> watches = new HashSet<>();

    // these fields may be updated
    ByteArray key;
    KvNodeHolder nodeHolder;
    WatchHolder parentWatchHolder;
    long lastRemoveIndex; // only used when mount to parent dir

    HashMap<ByteArray, WatchHolder> children;

    boolean waitingDispatch;

    WatchHolder(ByteArray key, KvNodeHolder nodeHolder, WatchHolder parentWatchHolder) {
        this.key = key;
        this.nodeHolder = nodeHolder;
        this.parentWatchHolder = parentWatchHolder;
        if (nodeHolder == null) {
            while (parentWatchHolder.nodeHolder == null) {
                parentWatchHolder = parentWatchHolder.parentWatchHolder;
            }
            this.lastRemoveIndex = parentWatchHolder.nodeHolder.latest.updateIndex;
        } else {
            this.lastRemoveIndex = nodeHolder.latest.removed ? nodeHolder.latest.updateIndex : 0;
        }
    }

    public boolean isNoUse() {
        return watches.isEmpty() && (children == null || children.isEmpty());
    }

    public WatchHolder getChild(ByteArray key) {
        if (children == null) {
            return null;
        }
        return children.get(key);
    }

    public void addChild(ByteArray key, WatchHolder child) {
        if (children == null) {
            children = new HashMap<>();
        }
        if (children.put(key, child) != null) {
            BugLog.log(new RaftException("watch holder child key already exists: " + key));
        }
    }

    public void removeChild(ByteArray key) {
        if (children == null) {
            BugLog.log(new RaftException("assert children != null"));
            return;
        }
        children.remove(key);
        if (children.isEmpty()) {
            children = null;
        }
    }
}

final class WatchNotifyRespCallback extends PbCallback<WatchNotifyRespCallback> {
    private static final int IDX_RESULTS = 1;

    final int[] results;
    private int nextWriteIndex;

    WatchNotifyRespCallback(int size) {
        this.results = new int[size];
    }

    @Override
    protected WatchNotifyRespCallback getResult() {
        return this;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        if (index == IDX_RESULTS) {
            if (nextWriteIndex >= results.length) {
                throw new RaftException("response results size exceed " + results.length);
            }
            results[nextWriteIndex] = (int) value;
            nextWriteIndex++;
            return true;
        }
        return false;
    }
}
