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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

/**
 * @author huangli
 */
public class Fiber extends WaitSource {
    private static final DtLog log = DtLogs.getLogger(Fiber.class);
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

    WaitSource source;

    @SuppressWarnings("rawtypes")
    FiberFrame stackTop;

    Object inputObj;
    Throwable inputEx;

    public Fiber(String fiberName, FiberGroup fiberGroup, FiberFrame<Void> entryFrame) {
        this(fiberName, fiberGroup, entryFrame, false);
    }

    public Fiber(String fiberName, FiberGroup fiberGroup, FiberFrame<Void> entryFrame, boolean daemon) {
        super(fiberGroup);
        this.fiberName = fiberName;
        this.stackTop = entryFrame;
        this.daemon = daemon;
        entryFrame.init(this);
    }

    public static <O2> FrameCallResult call(FiberFrame<O2> subFrame, FrameCall<O2> resumePoint) {
        Dispatcher.call(subFrame, resumePoint);
        return FrameCallResult.CALL_NEXT_FRAME;
    }

    public static <O2> FrameCallResult resume(O2 input, FrameCall<O2> resumePoint) throws Throwable {
        resumePoint.execute(input);
        return FrameCallResult.RETURN;
    }

    public static FrameCallResult sleep(long millis, FrameCall<Void> resumePoint) {
        DtUtil.checkPositive(millis, "millis");
        Dispatcher.sleep(millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public static FrameCallResult sleepUntilShouldStop(long millis, FrameCall<Void> resumePoint) {
        DtUtil.checkPositive(millis, "millis");
        Dispatcher.sleepUntilShouldStop(millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    public static FrameCallResult yield(FrameCall<Void> resumePoint) {
        Dispatcher.yield(resumePoint);
        return FrameCallResult.RETURN;
    }

    public static FrameCallResult fatal(Throwable ex) {
        log.error("encountered fatal error, raft group will shutdown", ex);
        DispatcherThread t = DispatcherThread.currentDispatcherThread();
        t.currentGroup.requestShutdown();
        throw new FiberException("encountered fatal error, raft group will shutdown", ex);
    }

    public static FrameCallResult frameReturn() {
        return FrameCallResult.RETURN;
    }

    @SuppressWarnings("rawtypes")
    void popFrame() {
        if (stackTop != null) {
            FiberFrame f = stackTop;
            stackTop = f.prev;
            f.prev = null;
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

    @Override
    protected boolean shouldWait(Fiber currentFiber) {
        return !finished;
    }

    @Override
    protected boolean throwWhenTimeout() {
        return false;
    }

    @Override
    protected void prepare(Fiber currentFiber, FiberFrame<?> currentFrame) {
        Object inputObj;
        if (currentFiber.scheduleTimeoutMillis >= 0) {
            inputObj = finished ? Boolean.TRUE : Boolean.FALSE;
        } else {
            inputObj = null;
        }
        currentFiber.inputObj = inputObj;
    }

    public FrameCallResult join(FrameCall<Void> resumePoint) {
        Fiber currentFibber = check();
        return Dispatcher.awaitOn(currentFibber, this, -1, resumePoint);
    }

    public FrameCallResult join(long millis, FrameCall<Boolean> resumePoint) {
        DtUtil.checkPositive(millis, "millis");
        Fiber currentFibber = check();
        return Dispatcher.awaitOn(currentFibber, this, millis, resumePoint);
    }

    public FiberFuture<Void> join() {
        check();
        Fiber waitSource = this;
        FiberFuture<Void> fu = fiberGroup.newFuture();
        FiberFrame<Void> entryFrame = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return waitSource.join(this::afterJoin);
            }

            private FrameCallResult afterJoin(Void result) {
                fu.complete(null);
                return Fiber.frameReturn();
            }
        };
        Fiber f = new Fiber("wait-finish", fiberGroup, entryFrame) {
            private String toStr;

            @Override
            public String toString() {
                if (toStr == null) {
                    toStr = super.toString() + "-" + waitSource;
                }
                return toStr;
            }
        };
        fiberGroup.start(f);
        return fu;
    }

    private Fiber check() {
        if (!started) {
            throw new FiberException("fiber not started");
        }
        Fiber fiber = Dispatcher.getCurrentFiberAndCheck(fiberGroup);
        if (fiber == this) {
            throw new FiberException("can't join self");
        }
        return fiber;
    }

    public void start() {
        fiberGroup.checkThread();
        fiberGroup.start(Fiber.this);
    }

    public boolean isStarted() {
        fiberGroup.checkThread();
        return started;
    }

    public boolean isFinished() {
        fiberGroup.checkThread();
        return finished;
    }

    public String getFiberName() {
        return fiberName;
    }

    public FiberGroup getFiberGroup() {
        return fiberGroup;
    }

    @Override
    public String toString() {
        return "Fiber " + fiberName;
    }
}
