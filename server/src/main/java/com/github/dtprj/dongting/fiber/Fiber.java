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
    protected final boolean daemon;

    long scheduleTimeoutMillis;
    long scheduleNanoTime;

    boolean started;
    boolean ready;
    boolean finished;

    boolean interrupted;

    WaitSource source;
    FiberCondition[] sourceConditions;

    @SuppressWarnings("rawtypes")
    FiberFrame stackTop;

    Object inputObj;
    Throwable inputEx;

    final short signalCountInEachRound;
    // (round << 16) | signalCountInCurrentRound
    int roundInfo;

    public Fiber(String name, FiberGroup fiberGroup, FiberFrame<Void> entryFrame) {
        this(name, fiberGroup, entryFrame, false, 1);
    }

    public Fiber(String name, FiberGroup fiberGroup, FiberFrame<Void> entryFrame, boolean daemon) {
        this(name, fiberGroup, entryFrame, daemon, 1);
    }

    public Fiber(String name, FiberGroup fiberGroup, FiberFrame<Void> entryFrame, boolean daemon,
                 int signalCountInEachRound) {
        super(name, fiberGroup);
        this.stackTop = entryFrame;
        this.daemon = daemon;
        this.signalCountInEachRound = (short) signalCountInEachRound;
        this.roundInfo = this.signalCountInEachRound;
        entryFrame.init(this);
    }

    public static <T> FrameCallResult call(FiberFrame<T> subFrame, FrameCall<T> resumePoint) {
        Dispatcher.call(subFrame, resumePoint);
        return FrameCallResult.CALL_NEXT_FRAME;
    }

    public static <I> FrameCallResult resume(I input, FrameCall<I> resumePoint) {
        Dispatcher.resume(input, null, resumePoint);
        return FrameCallResult.RETURN;
    }

    static FrameCallResult resumeEx(Throwable ex) {
        Dispatcher.resume(null, ex, null);
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

    public static FiberException fatal(Throwable ex) {
        DispatcherThread t = DispatcherThread.currentDispatcherThread();
        if (t.currentGroup.isShouldStop()) {
            return new FiberException("fatal ex", ex);
        } else {
            log.error("encountered fatal error, raft group will shutdown", ex);
            t.currentGroup.requestShutdown();
            return new FiberException("encountered fatal error, raft group will shutdown", ex);
        }
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
        fiberGroup.checkGroup();
        Dispatcher dispatcher = fiberGroup.dispatcher;
        dispatcher.interrupt(this);
    }

    @Override
    protected void prepare(Fiber waitFiber, boolean timeout) {
        if (waitFiber.scheduleTimeoutMillis > 0) {
            waitFiber.inputObj = timeout ? Boolean.FALSE : Boolean.TRUE;
        } else {
            waitFiber.inputObj = null;
        }
        waitFiber.source = null;
    }

    public FrameCallResult join(FrameCall<Void> resumePoint) {
        Fiber currentFiber = check();
        if (finished) {
            return Fiber.resume(null, resumePoint);
        }
        return Dispatcher.awaitOn(currentFiber, this, -1, resumePoint);
    }

    public FrameCallResult join(long millis, FrameCall<Boolean> resumePoint) {
        DtUtil.checkPositive(millis, "millis");
        Fiber currentFiber = check();
        if (finished) {
            return Fiber.resume(Boolean.TRUE, resumePoint);
        }
        return Dispatcher.awaitOn(currentFiber, this, millis, resumePoint);
    }

    public FiberFuture<Void> join() {
        check();
        if (finished) {
            return FiberFuture.completedFuture(fiberGroup, null);
        }
        Fiber waitSource = this;
        FiberFuture<Void> fu = fiberGroup.newFuture("join-" + this);
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
        Fiber f = new Fiber("wait-finish", fiberGroup, entryFrame, true) {
            private String toStr;

            @Override
            public String toString() {
                if (toStr == null) {
                    toStr = super.toString() + "-" + waitSource;
                }
                return toStr;
            }
        };
        fiberGroup.start(f, false);
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
        fiberGroup.checkGroup();
        fiberGroup.start(Fiber.this, false);
    }

    public boolean isStarted() {
        fiberGroup.checkGroup();
        return started;
    }

    public boolean isFinished() {
        fiberGroup.checkGroup();
        return finished;
    }

    void cleanSchedule() {
        scheduleTimeoutMillis = 0;
        scheduleNanoTime = 0;
    }

    public FiberGroup getFiberGroup() {
        return fiberGroup;
    }

    @Override
    public String toString() {
        return "Fiber:" + name + "@" + Integer.toHexString(hashCode());
    }
}
