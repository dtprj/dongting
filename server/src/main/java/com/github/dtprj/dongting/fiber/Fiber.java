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
public class Fiber {
    private static final DtLog log = DtLogs.getLogger(Fiber.class);
    protected final FiberGroup fiberGroup;
    protected final String fiberName;
    protected final boolean daemon;

    long scheduleTimeoutMillis;
    long scheduleNanoTime;

    Fiber previousWaiter;
    Fiber nextWaiter;

    boolean started;
    boolean ready;
    boolean finished;

    boolean interrupted;
    Throwable lastEx;

    WaitSource source;

    @SuppressWarnings("rawtypes")
    FiberFrame stackTop;

    public Fiber(String fiberName, FiberGroup fiberGroup, FiberFrame<Void> entryFrame) {
        this(fiberName, fiberGroup, entryFrame, false);
    }

    public Fiber(String fiberName, FiberGroup fiberGroup, FiberFrame<Void> entryFrame, boolean daemon) {
        this.fiberGroup = fiberGroup;
        this.fiberName = fiberName;
        this.stackTop = entryFrame;
        this.daemon = daemon;
        entryFrame.reset(this);
    }

    public static <O2> FrameCallResult call(FiberFrame<O2> subFrame, FrameCall<O2> resumePoint) {
        Dispatcher.call(subFrame, resumePoint);
        return FrameCallResult.CALL_NEXT_FRAME;
    }

    public static <O2> FrameCallResult resume(O2 input, FrameCall<O2> resumePoint) throws Exception {
        resumePoint.execute(input);
        return FrameCallResult.RETURN;
    }

    public static FrameCallResult sleep(long millis, FrameCall<Void> resumePoint) {
        Dispatcher.sleep(millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public static FrameCallResult sleepUntilShouldStop(long millis, FrameCall<Void> resumePoint) {
        Dispatcher.sleepUntilShouldStop(millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public static FrameCallResult fatal(Throwable ex) {
        log.error("encountered fatal error, raft group will shutdown", ex);
        DispatcherThead t = DispatcherThead.currentDispatcherThread();
        t.currentGroup.requestShutdown();
        throw new FiberException("encountered fatal error, raft group will shutdown", ex);
    }

    public static FrameCallResult frameReturn() {
        return FrameCallResult.RETURN;
    }

    @SuppressWarnings("rawtypes")
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

    @SuppressWarnings("rawtypes")
    void pushFrame(FiberFrame frame) {
        if (stackTop != null) {
            frame.prev = stackTop;
        }
        stackTop = frame;
    }

    public void interrupt() {
        fiberGroup.checkThread();
        Dispatcher dispatcher = fiberGroup.dispatcher;
        dispatcher.interrupt(this);
    }

    public void start() {
        fiberGroup.checkThread();
        fiberGroup.start(Fiber.this);
    }

    public String getFiberName() {
        return fiberName;
    }

    public FiberGroup getFiberGroup() {
        return fiberGroup;
    }

}
