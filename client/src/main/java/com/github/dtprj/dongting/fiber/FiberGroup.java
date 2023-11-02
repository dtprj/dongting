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
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class FiberGroup {
    private static final DtLog log = DtLogs.getLogger(FiberGroup.class);
    private final String name;
    final Dispatcher dispatcher;
    private final IndexedQueue<Fiber> readyQueue = new IndexedQueue<>(64);
    private final HashSet<Fiber> normalFibers = new HashSet<>();
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
    public CompletableFuture<Void> fireStart(Fiber f) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        dispatcher.getShareQueue().offer(() -> {
            if (shouldStop) {
                future.completeExceptionally(new IllegalStateException("fiber group already stopped"));
            } else {
                start(f);
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
    public void start(Fiber f) {
        if (!normalFibers.add(f)) {
            BugLog.getLog().error("fiber is in set: {}", f.getFiberName());
        }
        makeReady(f);
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

    void removeFiber(Fiber f) {
        if (!normalFibers.remove(f)) {
            BugLog.getLog().error("fiber is not in set: {}", f.getFiberName());
        }
    }

    void makeReady(Fiber f) {
        if (finished()) {
            log.warn("group finished, ignore makeReady: {}", f.getFiberName());
            return;
        }
        if (f.isFinished()) {
            log.warn("fiber already finished, ignore makeReady: {}", f.getFiberName());
            return;
        }
        if (!f.isReady()) {
            f.setReady();
            readyQueue.addLast(f);
        }
    }

    boolean finished() {
        return shouldStop && normalFibers.isEmpty();
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
