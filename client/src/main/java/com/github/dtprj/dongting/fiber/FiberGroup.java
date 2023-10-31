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
package com.github.dtprj.dongting.fiber;

import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class FiberGroup {
    private static final DtLog log = DtLogs.getLogger(FiberGroup.class);
    private final String name;
    private final Dispatcher dispatcher;
    private final IndexedQueue<Fiber> readyQueue = new IndexedQueue<>(64);
    private final HashSet<Fiber> normalFibers = new HashSet<>();
    private final HashSet<Fiber> daemonFibers = new HashSet<>();
    private final IntObjMap<FiberChannel<Object>> channels = new IntObjMap<>();

    private boolean shouldStop = false;

    FiberGroup(String name, Dispatcher dispatcher) {
        this.name = name;
        this.dispatcher = dispatcher;
    }

    /**
     * can call in any thread
     */
    public void fireMessage(int type, Object data) {
        dispatcher.getShareQueue().offer(() -> {
            FiberChannel<Object> c = channels.get(type);
            if (c == null) {
                log.warn("channel not found: {}", type);
                return;
            }
            c.offer(data);
        });
    }

    /**
     * can call in any thread
     */
    public CompletableFuture<Void> fireBound(Fiber f) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        dispatcher.getShareQueue().offer(() -> {
            if (shouldStop) {
                future.completeExceptionally(new IllegalStateException("fiber group already stopped"));
            } else {
                start(f);
                makeReady(f, false);
                future.complete(null);
            }
        });
        return future;
    }

    /**
     * can call in any thread
     */
    public void fireShutdown() {
        dispatcher.getShareQueue().offer(() -> shouldStop = true);
    }

    boolean isInGroupThread() {
        return Thread.currentThread() == dispatcher.getThread();
    }

    /**
     * should call in dispatch thread
     */
    public void bound(Fiber f, boolean addToFirst) {
        start(f);
        makeReady(f, addToFirst);
    }

    @SuppressWarnings("unchecked")
    public <T> FiberChannel<T> createOrGetChannel(int type) {
        FiberChannel<Object> channel = channels.get(type);
        if (channel == null) {
            channel = new FiberChannel<>(this);
        }
        channels.put(type, channel);
        return (FiberChannel<T>) channel;
    }

    /**
     * should call in dispatch thread
     */
    public Fiber getCurrentFiber() {
        return dispatcher.getCurrentFiber();
    }

    /**
     * should call in dispatch thread
     */
    public boolean isShouldStop() {
        return shouldStop;
    }

    public String getName() {
        return name;
    }

    private void start(Fiber f) {
        boolean b;
        if (f.isDaemon()) {
            b = daemonFibers.add(f);
        } else {
            b = normalFibers.add(f);
        }
        if (!b) {
            BugLog.getLog().error("fiber is in set: {}", f.getName());
        }
    }

    void removeFiber(Fiber f) {
        boolean b;
        if (f.isDaemon()) {
            b = daemonFibers.remove(f);
        } else {
            b = normalFibers.remove(f);
        }
        if (!b) {
            BugLog.getLog().error("fiber is not in set: {}", f.getName());
        }
    }

    void makeReady(Fiber f, boolean addToFirst) {
        if (finished()) {
            log.warn("group finished, ignore makeReady: {}", f.getName());
            return;
        }
        if (f.isFinished()) {
            log.warn("fiber already finished, ignore makeReady: {}", f.getName());
            return;
        }
        if (!f.isReady()) {
            f.setReady();
            if (addToFirst) {
                readyQueue.addFirst(f);
            } else {
                readyQueue.addLast(f);
            }
        }
    }

    boolean finished() {
        return shouldStop && normalFibers.isEmpty();
    }

    void cleanDaemonFibers() {
        for (Iterator<Fiber> it = daemonFibers.iterator(); it.hasNext(); ) {
            Fiber f = it.next();
            it.remove();
            dispatcher.setCurrentFiber(f);
            try {
                f.finish();
            } finally {
                dispatcher.setCurrentFiber(null);
            }
        }
    }

    FiberCondition newCondition() {
        return new FiberCondition(this);
    }

    IndexedQueue<Fiber> getReadyQueue() {
        return readyQueue;
    }

    void setShouldStop() {
        this.shouldStop = true;
    }

    Dispatcher getDispatcher() {
        return dispatcher;
    }
}
