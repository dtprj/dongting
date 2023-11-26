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

import java.util.function.Consumer;

/**
 * @author huangli
 */
@SuppressWarnings("rawtypes")
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

    FiberFrame stackTop;

    public Fiber(String fiberName, FiberGroup fiberGroup, FiberFrame entryFrame) {
        this(fiberName, fiberGroup, entryFrame, false);
    }

    public Fiber(String fiberName, FiberGroup fiberGroup, FiberFrame entryFrame, boolean daemon) {
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

    public static FrameCallResult sleep(long millis, FrameCall<Void> resumePoint) {
        Dispatcher.sleep(millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public static FrameCallResult fatal(Throwable ex) throws Throwable {
        log.error("encountered fatal error, raft group will shutdown", ex);
        DispatcherThead t = DispatcherThead.currentDispatcherThread();
        t.currentGroup.requestShutdown();
        throw ex;
    }

    public static FrameCallResult frameReturn() {
        return FrameCallResult.RETURN;
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

    public void start(Consumer<Exception> startFailCallback) {
        try {
            fiberGroup.dispatcher.doInDispatcherThread(() -> fiberGroup.start(Fiber.this, startFailCallback));
        } catch (FiberException e) {
            if (startFailCallback != null) {
                try {
                    startFailCallback.accept(e);
                } catch (Throwable e1) {
                    log.error("start fiber fail", e);
                    log.error("startFailCallback fail", e1);
                }
            } else {
                log.error("start fiber fail", e);
            }
        }
    }

    public void start() {
        start(null);
    }

    public String getFiberName() {
        return fiberName;
    }

    public FiberGroup getFiberGroup() {
        return fiberGroup;
    }

}
