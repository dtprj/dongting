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
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class Dispatcher extends Thread {
    private static final DtLog log = DtLogs.getLogger(Dispatcher.class);

    private final LinkedBlockingQueue<Runnable> shareQueue = new LinkedBlockingQueue<>();
    private final ArrayList<FiberGroup> groups = new ArrayList<>();
    private final IndexedQueue<Integer> finishedGroups = new IndexedQueue<>(8);

    private final Timestamp ts = new Timestamp();

    private Fiber currentFiber;
    private Thread thread;

    private boolean poll = true;
    private int pollTimeout = 50;

    private boolean shouldStop = false;

    public Dispatcher(String name) {
        super(name);
    }

    public CompletableFuture<FiberGroup> createFiberGroup(String name) {
        CompletableFuture<FiberGroup> future = new CompletableFuture<>();
        FiberGroup g = new FiberGroup(name, this);
        shareQueue.offer(() -> {
            if (g.isShouldStop()) {
                future.completeExceptionally(new IllegalStateException("fiber group already stopped"));
            } else {
                groups.add(g);
                future.complete(g);
            }
        });
        return future;
    }

    public void requestShutdown() {
        shareQueue.offer(() -> {
            shouldStop = true;
            groups.forEach(g -> g.setShouldStop());
        });
    }

    @Override
    public void run() {
        this.thread = Thread.currentThread();
        ArrayList<Runnable> localData = new ArrayList<>(64);
        ArrayList<FiberGroup> groups = this.groups;
        while (!finished()) {
            pollAndRefreshTs(ts, localData);
            int len = localData.size();
            for (int i = 0; i < len; i++) {
                Runnable r = localData.get(i);
                r.run();
            }
            len = groups.size();
            for (int i = 0; i < len; i++) {
                FiberGroup g = groups.get(i);
                IndexedQueue<Fiber> readyQueue = g.getReadyQueue();
                while (readyQueue.size() > 0) {
                    currentFiber = readyQueue.removeFirst();
                    try {
                        FiberEntryPoint fep = currentFiber.getNextEntryPoint();
                        if (fep == null) {
                            log.error("fiber entry point is null: {}", currentFiber.getName());
                            currentFiber.finish();
                        } else {
                            try {
                                fep.execute();
                            } catch (Throwable e) {
                                log.error("fiber execute error", e);
                                currentFiber.finish();
                            }
                        }
                    } finally {
                        currentFiber = null;
                    }
                }
                if (g.finished()) {
                    log.info("fiber group finished: {}", g.getName());
                    g.cleanDaemonFibers();
                    finishedGroups.addLast(i);
                }
            }
            while (finishedGroups.size() > 0) {
                int idx = finishedGroups.removeLast();
                groups.remove(idx);
            }
        }
        log.info("fiber dispatcher exit: {}", getName());
    }

    private void pollAndRefreshTs(Timestamp ts, ArrayList<Runnable> localData) {
        try {
            long oldNanos = ts.getNanoTime();
            if (poll) {
                Runnable o = shareQueue.poll(pollTimeout, TimeUnit.MILLISECONDS);
                if (o != null) {
                    localData.add(o);
                }
            } else {
                shareQueue.drainTo(localData);
            }

            ts.refresh(1);
            poll = ts.getNanoTime() - oldNanos > 2_000_000 || localData.isEmpty();
        } catch (InterruptedException e) {
            log.info("fiber dispatcher receive interrupt signal");
            pollTimeout = 1;
        }
    }

    private boolean finished() {
        return shouldStop && groups.isEmpty();
    }

    LinkedBlockingQueue<Runnable> getShareQueue() {
        return shareQueue;
    }

    Fiber getCurrentFiber() {
        return currentFiber;
    }

    void setCurrentFiber(Fiber currentFiber) {
        this.currentFiber = currentFiber;
    }

    Thread getThread() {
        return thread;
    }
}
