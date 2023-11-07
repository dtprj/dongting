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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

/**
 * @author huangli
 */
abstract class WaitSource {
    private static final DtLog log = DtLogs.getLogger(FiberCondition.class);
    private Fiber firstWaiter;
    private Fiber lastWaiter;
    protected final FiberGroup group;

    public WaitSource(FiberGroup group) {
        this.group = group;
    }

    void addWaiter(Fiber f) {
        if (firstWaiter == null) {
            firstWaiter = f;
        } else {
            lastWaiter.nextWaiter = f;
        }
        lastWaiter = f;
    }

    void removeWaiter(Fiber f) {
        if (firstWaiter == f) {
            firstWaiter = f.nextWaiter;
            if (firstWaiter == null) {
                lastWaiter = null;
            }
        } else {
            Fiber prev = firstWaiter;
            Fiber cur = firstWaiter.nextWaiter;
            while (cur != null) {
                if (cur == f) {
                    prev.nextWaiter = cur.nextWaiter;
                    if (cur == lastWaiter) {
                        lastWaiter = prev;
                    }
                    break;
                }
                prev = cur;
                cur = cur.nextWaiter;
            }
        }
    }

    private Fiber popNextWaiter() {
        Fiber f = firstWaiter;
        if (f == null) {
            return null;
        } else {
            firstWaiter = f.nextWaiter;
            if (firstWaiter == null) {
                lastWaiter = null;
            }
            f.nextWaiter = null;
            return f;
        }
    }

    void signal0() {
        if (group.finished) {
            return;
        }
        Fiber f = popNextWaiter();
        if (f != null) {
            group.makeFiberReady(f);
        }
    }

    void signalAll0() {
        if (group.finished) {
            return;
        }
        Fiber f;
        while ((f = popNextWaiter()) != null) {
            group.makeFiberReady(f);
        }
    }
}
