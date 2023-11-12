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

/**
 * @author huangli
 */
public class Fiber {
    protected final FiberGroup fiberGroup;
    protected final String fiberName;
    protected final boolean daemon;

    long scheduleTimeoutMillis;
    long scheduleNanoTime;

    Fiber nextWaiter;

    boolean started;
    boolean ready;
    boolean finished;

    boolean interrupted;
    Throwable lastEx;

    WaitSource source;

    FiberFrame stackTop;

    public Fiber(String fiberName, FiberGroup fiberGroup, boolean daemon) {
        this.fiberGroup = fiberGroup;
        this.fiberName = fiberName;
        this.daemon = daemon;
    }

    FiberFrame popFrame() {
        if (stackTop == null) {
            return null;
        } else {
            FiberFrame f = stackTop;
            stackTop = f.prev;
            f.prev = null;
            return f;
        }
    }

    void pushFrame(FiberFrame frame) {
        if (stackTop != null) {
            frame.prev = stackTop;
        }
        stackTop = frame;
    }

    public void interrupt() {
        Dispatcher dispatcher = fiberGroup.dispatcher;
        Fiber f = this;
        dispatcher.doInDispatcherThread(() -> dispatcher.interrupt(f));
    }

    public void start(FiberFrame<Void, Void> firstFrame) {
        fiberGroup.dispatcher.doInDispatcherThread(() -> fiberGroup.start(Fiber.this, firstFrame));
    }

    public String getFiberName() {
        return fiberName;
    }

    public FiberGroup getFiberGroup() {
        return fiberGroup;
    }

    public boolean isGroupShouldStop() {
        return fiberGroup.shouldStop;
    }
}
