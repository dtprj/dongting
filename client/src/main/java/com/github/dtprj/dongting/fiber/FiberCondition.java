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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

/**
 * @author huangli
 */
public class FiberCondition {
    private static final DtLog log = DtLogs.getLogger(FiberCondition.class);
    private final IndexedQueue<Fiber> waitQueue = new IndexedQueue<>(8);
    private final FiberGroup group;

    public FiberCondition(FiberGroup group) {
        this.group = group;
    }

    IndexedQueue<Fiber> getWaitQueue() {
        return waitQueue;
    }

    public void signal() {
        if (group.isInGroupThread()) {
            signal0();
        } else {
            group.getDispatcher().getShareQueue().offer(() -> signal0());
        }
    }

    private void signal0() {
        if (group.finished()) {
            log.warn("group finished, ignore signal: {}", group.getName());
            return;
        }
        if (waitQueue.size() > 0) {
            Fiber f = waitQueue.removeFirst();
            group.makeReady(f, false);
        }
    }

    public void signalAll() {
        if (group.isInGroupThread()) {
            signalAll0();
        } else {
            group.getDispatcher().getShareQueue().offer(() -> signalAll0());
        }
    }

    private void signalAll0() {
        if (group.finished()) {
            log.warn("group finished, ignore signalAll: {}", group.getName());
            return;
        }
        while (waitQueue.size() > 0) {
            Fiber f = waitQueue.removeFirst();
            group.makeReady(f, false);
        }
    }
}
