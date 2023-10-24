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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class Dispatcher {
    private static final DtLog log = DtLogs.getLogger(Dispatcher.class);

    private final LinkedBlockingQueue<Event> shareQueue = new LinkedBlockingQueue<>();
    private final ArrayList<FiberGroup> groups = new ArrayList<>();
    private final IndexedQueue<Integer> finishedGroups = new IndexedQueue<>(8);

    private final Timestamp ts = new Timestamp();

    private boolean poll = true;
    private int pollTimeout = 50;

    public Dispatcher() {
    }

    public FiberGroup createFiberGroup() {
        FiberGroup g = new FiberGroup(shareQueue);
        shareQueue.offer(() -> groups.add(g));
        return g;
    }

    public void runLoop() {
        ArrayList<Event> localData = new ArrayList<>(64);
        ArrayList<FiberGroup> groups = this.groups;
        do {
            pollAndRefreshTs(ts, localData);
            int len = localData.size();
            for (int i = 0; i < len; i++) {
                Event e = localData.get(i);
                e.execute();
            }
            len = groups.size();
            for (int i = 0; i < len; i++) {
                FiberGroup g = groups.get(i);
                IndexedQueue<Fiber> readyQueue = g.getReadyQueue();
                while (readyQueue.size() > 0) {
                    Fiber f = readyQueue.removeFirst();
                    FiberEntryPoint fep = f.getNextEntryPoint();
                    fep.execute();
                }
                if (g.finished()) {
                    log.info("fiber group finished");
                    g.cleanDaemonFibers();
                    finishedGroups.addLast(i);
                }
            }
            while (finishedGroups.size() > 0) {
                int idx = finishedGroups.removeLast();
                groups.remove(idx);
            }
        } while (!groups.isEmpty());
        log.info("fiber dispatcher exit");
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
}
