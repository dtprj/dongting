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
import com.github.dtprj.dongting.log.BugLog;

import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class FiberGroup {
    private final String name;
    private final Dispatcher dispatcher;
    private final IndexedQueue<Fiber> readyQueue = new IndexedQueue<>(64);
    private final HashSet<Fiber> normalFibers = new HashSet<>();
    private final HashSet<Fiber> daemonFibers = new HashSet<>();

    private boolean shouldStop = false;

    FiberGroup(String name, Dispatcher dispatcher) {
        this.name = name;
        this.dispatcher = dispatcher;
    }

    /**
     * can call in any thread
     */
    public void fireEvent(Event e) {
        dispatcher.getShareQueue().offer(e);
    }

    /**
     * can call in any thread
     */
    public CompletableFuture<Void> fireBound(Fiber f) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        dispatcher.getShareQueue().offer(new Event(() -> {
            if (shouldStop) {
                future.completeExceptionally(new IllegalStateException("fiber group already stopped"));
            } else {
                start(f);
                makeReady(f);
                future.complete(null);
            }
        }));
        return future;
    }

    /**
     * can call in any thread
     */
    public void fireShutdown() {
        fireEvent(new Event(() -> shouldStop = true));
    }

    /**
     * should call in dispatch thread
     */
    public void bound(Fiber f) {
        start(f);
        makeReady(f);
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
            BugLog.getLog().error("fiber is in set: {}", f);
        }
    }

    void finish(Fiber f) {
        boolean b;
        if (f.isDaemon()) {
            b = daemonFibers.remove(f);
        } else {
            b = normalFibers.remove(f);
        }
        if (!b) {
            BugLog.getLog().error("fiber is not in set: {}", f);
        }
    }

    void makeReady(Fiber f) {
        if (f.isFinished()) {
            throw new IllegalStateException("fiber already finished: " + name);
        }
        if (!f.isReady()) {
            f.setReady();
            readyQueue.addLast(f);
        }
    }

    boolean finished() {
        return normalFibers.isEmpty();
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

    IndexedQueue<Fiber> getReadyQueue() {
        return readyQueue;
    }

    void setShouldStop(boolean shouldStop) {
        this.shouldStop = shouldStop;
    }

}
