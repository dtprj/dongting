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
public abstract class Fiber {
    private static final DtLog log = DtLogs.getLogger(Fiber.class);

    private final boolean daemon;
    private final FiberGroup group;
    private final String name;
    private boolean ready;
    private boolean finished;

    protected FiberEntryPoint nextEntryPoint;

    public Fiber(FiberGroup group, String name, boolean daemon) {
        this.group = group;
        this.name = name;
        this.daemon = daemon;
        this.nextEntryPoint = this::firstEntryPoint;
    }

    public abstract void firstEntryPoint();

    FiberEntryPoint getNextEntryPoint() {
        return nextEntryPoint;
    }

    public boolean isDaemon() {
        return daemon;
    }

    protected final void finish() {
        try {
            finished = true;
            group.finish(this);
            clean();
        } catch (Throwable e) {
            log.error("fiber finish error", e);
        }
    }

    protected abstract void clean();

    protected void awaitOn(Condition c, FiberEntryPoint resumeEntryPoint) {
        this.ready = false;
        this.nextEntryPoint = resumeEntryPoint;
        c.getWaitQueue().addLast(this);
    }

    protected Condition newCondition() {
        return new Condition(group);
    }

    public String getName() {
        return name;
    }

    boolean isReady() {
        return ready;
    }

    void setReady() {
        this.ready = true;
    }

    boolean isFinished() {
        return finished;
    }
}
