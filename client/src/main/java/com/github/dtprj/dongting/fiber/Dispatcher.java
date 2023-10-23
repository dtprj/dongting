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
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
@SuppressWarnings({"Convert2Diamond", "ForLoopReplaceableByForEach"})
public class Dispatcher {
    private static final DtLog log = DtLogs.getLogger(Dispatcher.class);
    private volatile boolean shouldStop = false;


    private final LinkedBlockingQueue<Event> shareQueue = new LinkedBlockingQueue<Event>();
    private final IndexedQueue<Fiber> readyQueue = new IndexedQueue<Fiber>(64);
    private final HashSet<Fiber> normalFibers = new HashSet<Fiber>();
    private final HashSet<Fiber> daemonFibers = new HashSet<Fiber>();

    private final Timestamp ts = new Timestamp();

    private boolean poll = true;
    private int pollTimeout = 50;

    public void runLoop() {
        ArrayList<Event> localData = new ArrayList<Event>(64);
        IndexedQueue<Fiber> readyQueue = this.readyQueue;
        do {
            pollAndRefreshTs(ts, localData);
            for (int i = 0; i < localData.size(); i++) {
                Event e = localData.get(i);
                e.execute();
            }
            while (readyQueue.size() > 0) {
                Fiber f = readyQueue.removeFirst();
                FiberEntryPoint fep = f.nextEntryPoint();
                fep.execute();
            }
        } while (!normalFibers.isEmpty());
    }

    private void pollAndRefreshTs(Timestamp ts, ArrayList<Event> localData) {
        try {
            long oldNanos = ts.getNanoTime();
            if (poll) {
                Event o = shareQueue.poll(pollTimeout, TimeUnit.MILLISECONDS);
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

    public void createNewFiber(Fiber f) {
        f.setDispatcher(this);
        shareQueue.offer(() -> {
            start(f);
            makeReady(f);
        });
    }

    public void fireEvent(Event e) {
        shareQueue.offer(e);
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
        if (!f.isReady()) {
            f.setReady();
            readyQueue.addLast(f);
        }
    }

    public void requestStop() {
        this.shouldStop = true;
    }

    public boolean isShouldStop() {
        return shouldStop;
    }
}
