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
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author huangli
 */
public class FiberGroup {
    private final LinkedBlockingQueue<Event> shareQueue;
    private final IndexedQueue<Fiber> readyQueue = new IndexedQueue<>(64);
    private final HashSet<Fiber> normalFibers = new HashSet<>();
    private final HashSet<Fiber> daemonFibers = new HashSet<>();

    private volatile boolean shouldStop = false;

    FiberGroup(LinkedBlockingQueue<Event> shareQueue) {
        this.shareQueue = shareQueue;
    }

    public void createNewFiber(Fiber f) {
        f.setGroup(this);
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
        f.clean();
    }

    void makeReady(Fiber f) {
        if (!f.isReady()) {
            f.setReady();
            readyQueue.addLast(f);
        }
    }

    public boolean finished() {
        return normalFibers.isEmpty();
    }

    public void cleanDaemonFibers() {
        for (Iterator<Fiber> it = daemonFibers.iterator(); it.hasNext(); ) {
            Fiber f = it.next();
            it.remove();
            f.clean();
        }
    }

    IndexedQueue<Fiber> getReadyQueue() {
        return readyQueue;
    }

    public void requestStop() {
        this.shouldStop = true;
    }

    public boolean isShouldStop() {
        return shouldStop;
    }
}
