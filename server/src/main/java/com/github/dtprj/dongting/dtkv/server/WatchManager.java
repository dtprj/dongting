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

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.WatchNotify;
import com.github.dtprj.dongting.dtkv.WatchNotifyPushReq;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.DtChannel;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.ReadPacket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class WatchManager {
    private static final DtLog log = DtLogs.getLogger(WatchManager.class);
    private final LinkedHashSet<ChannelInfo> needNotifyChannels = new LinkedHashSet<>();
    private final IdentityHashMap<DtChannel, ChannelInfo> channelInfoMap = new IdentityHashMap<>();
    private final PriorityQueue<ChannelInfo> retryQueue = new PriorityQueue<>();
    private ChannelInfo activeQueueHead;
    private ChannelInfo activeQueueTail;

    private final int groupId;
    private final Timestamp ts;
    private final Supplier<KvStatus> kvStatusSupplier;
    private final Executor executor;
    private final long[] retryIntervalNanos;
    private int epoch;

    static int maxBytesPerRequest = 80 * 1024; // may exceed

    private final ArrayList<Pair<Watch, WatchNotify>> tempList = new ArrayList<>(64);

    private static final FutureCallback<Integer> defaultCallback = (r, ex) -> {
        if (r != KvCodes.CODE_SUCCESS) {
            log.error("default callback failed. bizCode={}", r);
        }
    };

    WatchManager(int groupId, Timestamp ts, Supplier<KvStatus> kvStatusSupplier, Executor executor) {
        this(groupId, ts, kvStatusSupplier, executor, new long[]{1000, 10_000, 30_000, 60_000});
    }

    WatchManager(int groupId, Timestamp ts, Supplier<KvStatus> kvStatusSupplier, Executor executor,
                 long[] retryIntervalMillis) {
        this.groupId = groupId;
        this.ts = ts;
        this.kvStatusSupplier = kvStatusSupplier;
        this.executor = executor;
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

    private boolean checkInstallSnapshot(FutureCallback<Integer> callback, KvStatus kvStatus) {
        if (kvStatus.installSnapshot) {
            FutureCallback.callSuccess(callback, KvCodes.CODE_INSTALL_SNAPSHOT);
            return true;
        }
        return false;
    }

    // called by io processor thread
    public void addWatch(DtChannel channel, ByteArray[] keys, long[] notifiedIndex, int[] notifiedState,
                         FutureCallback<Integer> callback) {
        if (keys == null || notifiedIndex == null || notifiedState == null
                || keys.length != notifiedIndex.length || keys.length != notifiedState.length) {
            FutureCallback.callSuccess(callback, KvCodes.CODE_CLIENT_REQ_ERROR);
            return;
        }
        KvStatus kvStatus = kvStatusSupplier.get();
        if (checkInstallSnapshot(callback, kvStatus)) {
            return;
        }
        for (ByteArray key : keys) {
            KvImpl kv = kvStatus.kvImpl;
            int bc = kv.checkKey(key, false, true);
            if (bc != KvCodes.CODE_SUCCESS) {
                FutureCallback.callSuccess(callback, bc);
                return;
            }
        }
        executor.execute(() -> doAddWatch(channel, keys, notifiedIndex, notifiedState, callback));
    }

    private void doAddWatch(DtChannel channel, ByteArray[] keys, long[] notifiedIndex, int[] notifiedState,
                            FutureCallback<Integer> callback) {
        KvStatus kvStatus = kvStatusSupplier.get();
        if (checkInstallSnapshot(callback, kvStatus)) {
            return;
        }
        ChannelInfo ci = channelInfoMap.get(channel);
        if (ci == null) {
            ci = new ChannelInfo(channel);
            channelInfoMap.put(channel, ci);
        }
        addOrUpdateActiveQueue(ci);
        Watch[] watches = new Watch[keys.length];
        KvImpl kv = kvStatus.kvImpl;
        for (int i = 0; i < keys.length; i++) {
            ByteArray k = keys[i];
            Watch w = ci.watches.get(k);
            if (w == null) {
                w = createWatch(kv, k, ci, notifiedIndex[i], notifiedState[i]);
                ci.watches.put(w.watchHolder.key, w);
            }
            watches[i] = w;
        }
        prepareDispatch(ci, watches);
        FutureCallback.callSuccess(callback, KvCodes.CODE_SUCCESS);
    }

    private static Watch createWatch(KvImpl kv, ByteArray key, ChannelInfo ci, long notifiedIndex, int notifiedState) {
        KvNodeHolder nodeHolder = kv.map.get(key);
        if (nodeHolder != null) {
            if (nodeHolder.watchHolder == null) {
                nodeHolder.watchHolder = new WatchHolder(nodeHolder.key, nodeHolder, null);
            }
            Watch w = new Watch(nodeHolder.watchHolder, ci, notifiedIndex, notifiedState);
            nodeHolder.watchHolder.watches.add(w);
            return w;
        } else {
            // mount to parent dir
            ByteArray parentKey = key;
            while (true) {
                parentKey = kv.parentKey(parentKey);
                nodeHolder = kv.map.get(parentKey);
                if (nodeHolder != null) {
                    if (nodeHolder.watchHolder == null) {
                        nodeHolder.watchHolder = new WatchHolder(nodeHolder.key, nodeHolder, null);
                    }
                    break;
                }
            }
            WatchHolder watchHolder = nodeHolder.watchHolder;
            ByteArray childKey = kv.next(key, parentKey);
            while (true) {
                WatchHolder subWatchHolder = new WatchHolder(childKey, null, watchHolder);
                watchHolder.addChild(childKey, subWatchHolder);
                watchHolder = subWatchHolder;
                if (childKey == key) {
                    break;
                } else {
                    childKey = kv.next(childKey, parentKey);
                }
            }
            Watch w = new Watch(watchHolder, ci, notifiedIndex, notifiedState);
            watchHolder.watches.add(w);
            return w;
        }
    }

    private void prepareDispatch(ChannelInfo ci, Watch[] watches) {
        boolean add = false;
        for (Watch w : watches) {
            if (w.removed || w.pending) {
                continue;
            }
            ci.add(w);
            add = true;
        }
        if (add && ci.failCount == 0 && !ci.pending) {
            needNotifyChannels.add(ci);
        }
    }


    // called by io processor thread
    public void removeWatch(DtChannel channel, ByteArray[] keys, FutureCallback<Integer> callback) {
        if (checkInstallSnapshot(callback, kvStatusSupplier.get())) {
            return;
        }
        if (keys == null) {
            FutureCallback.callSuccess(callback, KvCodes.CODE_CLIENT_REQ_ERROR);
            return;
        }
        executor.execute(() -> {
            if (checkInstallSnapshot(callback, kvStatusSupplier.get())) {
                return;
            }
            ChannelInfo ci = channelInfoMap.get(channel);
            if (ci == null) {
                FutureCallback.callSuccess(callback, KvCodes.CODE_SUCCESS);
                return;
            }
            for (ByteArray key : keys) {
                Watch w = ci.watches.remove(key);
                if (w != null) {
                    removeWatchFromKvTree(w);
                }
            }
            if (ci.watches.isEmpty()) {
                // this channel has no watches, remove channel info
                ci.remove = true;
                channelInfoMap.remove(channel);
                removeFromActiveQueue(ci);
            }
            FutureCallback.callSuccess(callback, KvCodes.CODE_SUCCESS);
        });
    }

    private void removeWatchFromKvTree(Watch w) {
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

    // called by io processor thread
    public void removeWatchByChannel(DtChannel channel, FutureCallback<Integer> callback) {
        if (checkInstallSnapshot(callback, kvStatusSupplier.get())) {
            return;
        }
        executor.execute(() -> {
            if (checkInstallSnapshot(callback, kvStatusSupplier.get())) {
                return;
            }
            ChannelInfo ci = channelInfoMap.remove(channel);
            if (ci != null) {
                ci.remove = true;
                removeFromActiveQueue(ci);
                for (Watch w : ci.watches.values()) {
                    removeWatchFromKvTree(w);
                }
            }
            FutureCallback.callSuccess(callback, KvCodes.CODE_SUCCESS);
        });
    }

    public boolean dispatch() {
        try {
            int count = 0;
            if (!needNotifyChannels.isEmpty()) {
                Iterator<ChannelInfo> it = needNotifyChannels.iterator();
                while (it.hasNext()) {
                    ChannelInfo ci = it.next();
                    if (ci.failCount == 0) {
                        if (++count > 100) {
                            return false;
                        }
                        it.remove();
                        pushNotify(ci);
                    }
                }
                needNotifyChannels.clear();
            }
            ChannelInfo ci = retryQueue.peek();
            while (ci != null && ci.retryNanos - ts.nanoTime <= 0) {
                if (++count > 100) {
                    return false;
                }
                retryQueue.poll();
                pushNotify(ci);
                ci = retryQueue.peek();
            }
        } catch (Throwable e) {
            log.error("", e);
        }
        return true;
    }

    private void pushNotify(ChannelInfo ci) {
        if (ci.channel.getChannel().isOpen()) {
            removeWatchByChannel(ci.channel, defaultCallback);
        }
        if (ci.remove) {
            return;
        }
        Iterator<Watch> it = ci.notifyIterator();
        if (it == null) {
            return;
        }
        int bytes = 0;
        ArrayList<Pair<Watch, WatchNotify>> list = tempList;
        try {
            while (it.hasNext()) {
                Watch w = it.next();
                it.remove();
                if (w.removed || w.pending) {
                    continue;
                }
                WatchNotify wn = createNotify(w);
                if (wn != null) {
                    list.add(new Pair<>(w, wn));
                    w.pending = true;
                    bytes += wn.key.length + (wn.value == null ? 0 : wn.value.length);
                    if (bytes > maxBytesPerRequest) {
                        break;
                    }
                }
            }
            if (list.isEmpty()) {
                ci.pending = false;
            } else {
                ci.pending = true;
                ci.lastNotifyNanos = ts.nanoTime;
                ArrayList<Watch> watchList = new ArrayList<>(list.size());
                ArrayList<WatchNotify> notifyList = new ArrayList<>(list.size());
                for (Pair<Watch, WatchNotify> p : list) {
                    watchList.add(p.getLeft());
                    notifyList.add(p.getRight());
                }
                WatchNotifyPushReq req = new WatchNotifyPushReq(groupId, notifyList);
                EncodableBodyWritePacket r = new EncodableBodyWritePacket(Commands.DTKV_WATCH_NOTIFY_PUSH, req);
                DtTime timeout = new DtTime(5, TimeUnit.SECONDS);

                int requestEpoch = epoch;
                DecoderCallbackCreator<NotifyPushRespCallback> decoder =
                        ctx -> ctx.toDecoderCallback(new NotifyPushRespCallback(watchList.size()));
                boolean fireNext = it.hasNext();
                ((NioServer) ci.channel.getOwner()).sendRequest(ci.channel, r, decoder, timeout, (result, ex) ->
                        executor.execute(() -> processNotifyResult(ci, watchList, result, ex, requestEpoch, fireNext)));
            }
        } finally {
            list.clear();
        }
    }

    private WatchNotify createNotify(Watch w) {
        KvNodeHolder node = w.watchHolder.nodeHolder;
        if (node != null) {
            if (w.notifiedIndex >= node.latest.updateIndex) {
                return null;
            }
            byte[] key = node.key.getData();
            if (node.latest.removed) {
                return new WatchNotify(node.latest.updateIndex,
                        WatchNotify.RESULT_NOT_EXISTS, key, null);
            } else if (node.latest.isDir) {
                return new WatchNotify(node.latest.updateIndex,
                        WatchNotify.RESULT_DIRECTORY_EXISTS, key, null);
            } else {
                return new WatchNotify(node.latest.updateIndex,
                        WatchNotify.RESULT_VALUE_EXISTS, key, node.latest.data);
            }
        } else {
            if (w.notifiedIndex >= w.watchHolder.lastRemoveIndex) {
                return null;
            } else {
                return new WatchNotify(w.watchHolder.lastRemoveIndex,
                        WatchNotify.RESULT_NOT_EXISTS, w.watchHolder.key.getData(), null);
            }
        }
    }

    private void processNotifyResult(ChannelInfo ci, ArrayList<Watch> watches, ReadPacket<NotifyPushRespCallback> result,
                                     Throwable ex, int requestEpoch, boolean fireNext) {
        for (int size = watches.size(), i = 0; i < size; i++) {
            Watch w = watches.get(i);
            w.pending = false;
        }
        ci.pending = false;

        if (epoch != requestEpoch) {
            return;
        }
        if (ex != null) {
            log.warn("notify failed. remote={}, ex={}", ci.channel.getRemoteAddr(), ex);
            retryByChannel(ci, watches);
        } else if (result.getBizCode() == KvCodes.CODE_SUCCESS) {
            NotifyPushRespCallback callback = result.getBody();
            if (callback == null || callback.results.size() != watches.size()) {
                log.info("notify resp error. remote={}", ci.channel.getRemoteAddr());
                retryByChannel(ci, watches);
                return;
            }

            ci.failCount = 0;
            for (int size = watches.size(), i = 0; i < size; i++) {
                int bizCode = callback.results.get(i);
                Watch w = watches.get(i);
                if (bizCode == KvCodes.CODE_REMOVE_WATCH) {
                    removeWatch(ci.channel, new ByteArray[]{w.watchHolder.key}, defaultCallback);
                } else {
                    if (bizCode != KvCodes.CODE_SUCCESS) {
                        log.error("notify failed. remote={}, bizCode={}", ci.channel.getRemoteAddr(), bizCode);
                    }
                    ci.add(w); // remove in pushNotify(ChannelInfo) method
                }
            }
            if (fireNext) {
                pushNotify(ci);
            } else if (ci.needNotify != null) {
                if (!ci.needNotify.isEmpty()) {
                    needNotifyChannels.add(ci);
                } else if (ts.nanoTime - ci.lastNotifyNanos > 1_000_000_000L) {
                    ci.needNotify = null;
                }
            }
        } else if (result.getBizCode() == KvCodes.CODE_REMOVE_ALL_WATCH) {
            removeWatchByChannel(ci.channel, defaultCallback);
        } else {
            log.error("notify failed. remote={}, bizCode={}", ci.channel.getRemoteAddr(), result.getBizCode());
            retryByChannel(ci, watches);
        }
    }

    private void retryByChannel(ChannelInfo ci, ArrayList<Watch> watches) {
        ci.failCount++;
        int idx = Math.min(ci.failCount - 1, retryIntervalNanos.length - 1);
        ci.retryNanos = ts.nanoTime + retryIntervalNanos[idx];
        retryQueue.add(ci);
        for (int size = watches.size(), i = 0; i < size; i++) {
            ci.add(watches.get(i));
        }
    }

    public void cleanTimeoutChannel(long timeoutNanos) {
        try {
            while (activeQueueHead != null) {
                if (ts.nanoTime - activeQueueHead.lastNotifyNanos > timeoutNanos) {
                    removeWatchByChannel(activeQueueHead.channel, defaultCallback);
                }
            }
        } catch (Throwable e) {
            log.error("", e);
        }
    }

}

class ChannelInfo implements Comparable<ChannelInfo> {
    final DtChannel channel;
    final HashMap<ByteArray, Watch> watches = new HashMap<>(4);

    ChannelInfo prev;
    ChannelInfo next;

    boolean pending;
    long lastNotifyNanos;

    HashSet<Watch> needNotify;

    long retryNanos;
    int failCount;

    boolean remove;

    ChannelInfo(DtChannel channel) {
        this.channel = channel;
    }

    @Override
    public int compareTo(ChannelInfo o) {
        long diff = retryNanos - o.retryNanos;
        return diff < 0 ? -1 : (diff > 0 ? 1 : 0);
    }

    public void add(Watch w) {
        if (needNotify == null) {
            needNotify = new LinkedHashSet<>();
        }
        needNotify.add(w);
    }

    public Iterator<Watch> notifyIterator() {
        if (needNotify == null) {
            return null;
        } else {
            return needNotify.iterator();
        }
    }

}

class Watch {
    final WatchHolder watchHolder;
    final ChannelInfo channelInfo;

    long notifiedIndex;
    int notifiedState;

    boolean pending;
    boolean removed;

    Watch(WatchHolder watchHolder, ChannelInfo channelInfo, long notifiedIndex, int notifiedState) {
        this.watchHolder = watchHolder;
        this.channelInfo = channelInfo;
        this.notifiedIndex = notifiedIndex;
        this.notifiedState = notifiedState;
    }
}

class WatchHolder {
    final HashSet<Watch> watches = new HashSet<>();

    // these fields may be updated
    ByteArray key;
    KvNodeHolder nodeHolder;
    WatchHolder parentWatchHolder;
    long lastRemoveIndex; // only used when mount to parent dir

    private HashMap<ByteArray, WatchHolder> children;

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

    public HashMap<ByteArray, WatchHolder> createOrGetSubWatchMap() {
        if (children == null) {
            children = new HashMap<>();
        }
        return children;
    }

    public void addChild(ByteArray key, WatchHolder child) {
        if (children == null) {
            children = new HashMap<>();
        }
        children.put(key, child);
    }

    public void removeChild(ByteArray key) {
        // assert children != null;
        children.remove(key);
        if (children.isEmpty()) {
            children = null;
        }
    }
}

class NotifyPushRespCallback extends PbCallback<NotifyPushRespCallback> {
    private static final int IDX_RESULTS = 1;

    final ArrayList<Integer> results;

    NotifyPushRespCallback(int size) {
        this.results = new ArrayList<>(size);
    }

    @Override
    protected NotifyPushRespCallback getResult() {
        return this;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        if (index == IDX_RESULTS) {
            results.add((int) value);
            return true;
        }
        return false;
    }
}
