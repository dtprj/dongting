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

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Dispatcher extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(Dispatcher.class);

    private final FiberQueue shareQueue = new FiberQueue();
    private final ArrayList<FiberGroup> groups = new ArrayList<>();
    private final ArrayList<FiberGroup> finishedGroups = new ArrayList<>();
    final IndexedQueue<FiberGroup> readyGroups = new IndexedQueue<>(8);
    private final PriorityQueue<Fiber> scheduleQueue = new PriorityQueue<>(this::compareFiberByScheduleTime);

    private final Timestamp ts = new Timestamp();

    final DispatcherThead thread;

    private boolean poll = true;
    private long pollTimeout = TimeUnit.MILLISECONDS.toNanos(50);

    private boolean shouldStop = false;

    Object inputObj;
    // fatal error will cause fiber exit
    private Throwable fatalError;

    public Dispatcher(String name) {
        thread = new DispatcherThead(this::run, name);
    }

    public CompletableFuture<Void> startGroup(FiberGroup fiberGroup) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        boolean b = shareQueue.offer(new FiberQueueTask() {
            @Override
            protected void run() {
                if (shouldStop) {
                    future.completeExceptionally(new FiberException("dispatcher should stop"));
                } else {
                    groups.add(fiberGroup);
                    future.complete(null);
                }
            }
        });
        if (!b) {
            future.completeExceptionally(new FiberException("dispatcher already stopped"));
        }
        return future;
    }

    @Override
    protected void doStart() {
        thread.start();
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
        shareQueue.offer(new FiberQueueTask() {
            @Override
            protected void run() {
                shouldStop = true;
                groups.forEach(FiberGroup::requestShutdown);
            }
        });
    }

    private void run() {
        ArrayList<FiberQueueTask> localData = new ArrayList<>(64);
        while (!finished()) {
            runImpl(localData);
        }
        shareQueue.shutdown();
        runImpl(localData);
        log.info("fiber dispatcher exit: {}", thread.getName());
    }

    private void runImpl(ArrayList<FiberQueueTask> localData) {
        pollAndRefreshTs(ts, localData);
        processScheduleFibers();
        int len = localData.size();
        for (int i = 0; i < len; i++) {
            try {
                FiberQueueTask r = localData.get(i);
                r.run();
            } catch (Throwable e) {
                log.error("dispatcher run task fail", e);
            }
        }
        localData.clear();

        len = readyGroups.size();
        for (int i = 0; i < len; i++) {
            FiberGroup g = readyGroups.removeFirst();
            execGroup(g);
            if (g.ready) {
                readyGroups.addLast(g);
            }
        }
        if (!finishedGroups.isEmpty()) {
            groups.removeAll(finishedGroups);
        }
    }

    private void processScheduleFibers() {
        long now = ts.getNanoTime();
        PriorityQueue<Fiber> scheduleQueue = this.scheduleQueue;
        while (true) {
            Fiber f = scheduleQueue.peek();
            if (f == null || f.scheduleNanoTime - now > 0) {
                break;
            }
            scheduleQueue.poll();
            if (f.fiberGroup.finished) {
                if (!f.daemon) {
                    BugLog.getLog().error("group finished, but suspend fiber is not daemon: {}", f.getFiberName());
                }
                continue;
            }
            if (f.source != null) {
                f.source.removeWaiter(f);
                if (f.source instanceof FiberFuture) {
                    f.lastEx = new FiberTimeoutException("wait " + f.source + "timeout:" + f.scheduleTimeoutMillis + "ms");
                    f.stackTop.resumePoint = null;
                }
                f.source = null;
            }
            f.scheduleTimeoutMillis = 0;
            f.scheduleNanoTime = 0;
            f.fiberGroup.tryMakeFiberReady(f, true);
        }
    }

    private void execGroup(FiberGroup g) {
        thread.currentGroup = g;
        try {
            IndexedQueue<Fiber> readyQueue = g.readyFibers;
            int size = readyQueue.size();
            for (int i = 0; i < size; i++) {
                Fiber fiber = readyQueue.removeFirst();
                execFiber(g, fiber);
            }
            if (readyQueue.size() > 0) {
                poll = false;
            }
            if (g.finished) {
                log.info("fiber group finished: {}", g.getName());
                finishedGroups.add(g);
                g.ready = false;
            } else {
                g.ready = readyQueue.size() > 0;
            }
        } finally {
            thread.currentGroup = null;
        }
    }

    private void execFiber(FiberGroup g, Fiber fiber) {
        try {
            g.currentFiber = fiber;
            FiberFrame currentFrame = fiber.stackTop;
            while (currentFrame != null) {
                if (fiber.source != null) {
                    if (fiber.source instanceof FiberFuture) {
                        FiberFuture fu = (FiberFuture) fiber.source;
                        if (fu.execEx != null) {
                            fiber.lastEx = fu.execEx;
                            currentFrame.resumePoint = null;
                        } else {
                            inputObj = fu.execResult;
                        }
                    }
                    fiber.source = null;
                }
                processFrame(fiber, currentFrame);
                if (fatalError != null) {
                    fiber.lastEx = fatalError;
                    break;
                }
                if (!fiber.ready) {
                    return;
                }
                if (currentFrame == fiber.stackTop) {
                    if (fiber.source != null) {
                        // called awaitOn() on a completed future or get lock
                        if (fiber.lastEx == null) {
                            continue;
                        } else {
                            // user code throws ex after called awaitOn
                            BugLog.getLog().error("usage fatal error: throw ex after suspend call", fiber.lastEx);
                            break;
                        }
                    }
                    inputObj = currentFrame.result;
                    fiber.popFrame(); // remove self
                } else {
                    // call new frame
                    if (fiber.lastEx != null) {
                        BugLog.getLog().error("usage fatal error: suspend call should be last statement");
                        fiber.lastEx = new FiberException(
                                "usage fatal error: suspend call should be last statement", fiber.lastEx);
                        break;
                    }
                }
                currentFrame = fiber.stackTop;
            }
            if (fiber.lastEx != null) {
                log.error("fiber execute error, group={}, fiber={}", g.getName(), fiber.getFiberName(), fiber.lastEx);
            }
            fiber.finished = true;
            fiber.ready = false;
            g.removeFiber(fiber);
        } finally {
            inputObj = null;
            g.currentFiber = null;
            fiber.lastEx = null;
            fatalError = null;
        }
    }

    private void processFrame(Fiber fiber, FiberFrame currentFrame) {
        try {
            if (fiber.lastEx != null) {
                tryHandleEx(currentFrame, fiber.lastEx);
            } else {
                try {
                    Object input = inputObj;
                    inputObj = null;
                    if (currentFrame.status < FiberFrame.STATUS_BODY_CALLED) {
                        currentFrame.status = FiberFrame.STATUS_BODY_CALLED;
                    }
                    FrameCall r = currentFrame.resumePoint;
                    currentFrame.resumePoint = null;
                    r.execute(input);
                } catch (Throwable e) {
                    tryHandleEx(currentFrame, e);
                }
            }
        } finally {
            try {
                if (currentFrame.status < FiberFrame.STATUS_FINALLY_CALLED && currentFrame.resumePoint == null) {
                    currentFrame.status = FiberFrame.STATUS_FINALLY_CALLED;
                    currentFrame.doFinally();
                }
            } catch (Throwable e) {
                fiber.lastEx = e;
            }
        }
    }

    private void tryHandleEx(FiberFrame currentFrame, Throwable x) {
        currentFrame.resumePoint = null;
        Fiber fiber = currentFrame.fiber;
        if (currentFrame.status < FiberFrame.STATUS_CATCH_CALLED) {
            currentFrame.status = FiberFrame.STATUS_CATCH_CALLED;
            fiber.lastEx = null;
            try {
                currentFrame.handle(x);
            } catch (Throwable e) {
                fiber.lastEx = e;
            }
        } else {
            fiber.lastEx = x;
        }
    }

    static void call(FiberFrame subFrame, FrameCall resumePoint) {
        Fiber fiber = checkAndGetCurrentFiber();
        checkReentry(fiber);
        FiberFrame currentFrame = fiber.stackTop;
        currentFrame.resumePoint = resumePoint;
        subFrame.reset(fiber);
        fiber.fiberGroup.dispatcher.inputObj = null;
        fiber.pushFrame(subFrame);
    }

    static FrameCallResult awaitOn(WaitSource c, long millis, FrameCall resumePoint) {
        Fiber fiber = checkAndGetCurrentFiber();
        return awaitOn(fiber, c, millis, resumePoint);
    }

    static FrameCallResult awaitOn(Fiber fiber, WaitSource c, long millis, FrameCall resumePoint) {
        checkInterrupt(fiber);
        checkReentry(fiber);
        FiberFrame currentFrame = fiber.stackTop;
        currentFrame.resumePoint = resumePoint;
        fiber.source = c;
        if (!c.shouldWait(fiber)) {
            return FrameCallResult.RETURN;
        }
        fiber.ready = false;
        if (millis > 0) {
            fiber.fiberGroup.dispatcher.addToScheduleQueue(millis, fiber);
        }
        c.addWaiter(fiber);
        return FrameCallResult.SUSPEND;
    }

    static void sleep(long millis, FrameCall<Void> resumePoint) {
        Fiber fiber = checkAndGetCurrentFiber();
        checkInterrupt(fiber);
        checkReentry(fiber);
        FiberFrame currentFrame = fiber.stackTop;
        currentFrame.resumePoint = resumePoint;
        fiber.ready = false;
        fiber.fiberGroup.dispatcher.addToScheduleQueue(millis, fiber);
    }

    static void sleepUntilShouldStop(long millis, FrameCall<Void> resumePoint) {
        Fiber fiber = checkAndGetCurrentFiber();
        checkInterrupt(fiber);
        checkReentry(fiber);
        FiberFrame currentFrame = fiber.stackTop;
        currentFrame.resumePoint = resumePoint;
        FiberGroup g = fiber.fiberGroup;
        if (g.isShouldStopPlain()) {
            return;
        }
        fiber.source = g.shouldStopCondition;
        g.shouldStopCondition.addWaiter(fiber);
        fiber.ready = false;
        fiber.fiberGroup.dispatcher.addToScheduleQueue(millis, fiber);
    }

    private void addToScheduleQueue(long millis, Fiber fiber) {
        fiber.scheduleTimeoutMillis = millis;
        fiber.scheduleNanoTime = ts.getNanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        scheduleQueue.add(fiber);
    }

    static Fiber checkAndGetCurrentFiber() {
        DispatcherThead dispatcherThead = DispatcherThead.currentDispatcherThread();
        Fiber fiber = dispatcherThead.currentGroup.currentFiber;
        if (!fiber.ready) {
            throwFatalError(dispatcherThead.currentGroup, "usage fatal error: current fiber not ready state");
        }
        return fiber;
    }

    private static void checkReentry(Fiber fiber) {
        if (fiber.stackTop.resumePoint != null) {
            throwFatalError(fiber.fiberGroup, "usage fatal error: " +
                    "current frame resume point is not null, may invoke call()/awaitOn()/sleep() twice, " +
                    "or not return after invoke");
        }
    }

    private static void checkInterrupt(Fiber fiber) {
        if (fiber.interrupted) {
            fiber.interrupted = false;
            throw new FiberInterruptException("fiber is interrupted");
        }
    }

    private static void throwFatalError(FiberGroup g, String msg) {
        FiberException fe = new FiberException(msg);
        g.dispatcher.fatalError = fe;
        g.requestShutdown();
        throw fe;
    }

    void tryRemoveFromScheduleQueue(Fiber f) {
        if (f.scheduleTimeoutMillis > 0) {
            f.scheduleTimeoutMillis = 0;
            f.scheduleNanoTime = 0;
            scheduleQueue.remove(f);
        }
    }

    void interrupt(Fiber fiber) {
        if (fiber.finished || fiber.fiberGroup.finished) {
            return;
        }
        if (!fiber.started || fiber.ready) {
            fiber.interrupted = true;
        } else {
            String str;
            if (fiber.source != null) {
                WaitSource s = fiber.source;
                s.removeWaiter(fiber);
                fiber.source = null;
                if (s instanceof FiberFuture) {
                    str = "future " + s;
                } else if (s instanceof FiberCondition) {
                    str = "condition " + s;
                } else {
                    str = "lock " + s;
                }
            } else {
                str = "sleep";
            }
            fiber.stackTop.resumePoint = null;
            fiber.interrupted = false;
            fiber.lastEx = new FiberInterruptException("fiber is interrupted during wait " + str);
            tryRemoveFromScheduleQueue(fiber);
            fiber.fiberGroup.tryMakeFiberReady(fiber, false);
        }
    }

    private void pollAndRefreshTs(Timestamp ts, ArrayList<FiberQueueTask> localData) {
        try {
            long oldNanos = ts.getNanoTime();
            Fiber f = scheduleQueue.peek();
            long t = 0;
            if (f != null) {
                t = f.scheduleNanoTime - oldNanos;
            }
            if (poll && t > 0) {
                FiberQueueTask o = shareQueue.poll(Math.min(t, pollTimeout), TimeUnit.NANOSECONDS);
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
            pollTimeout = TimeUnit.MICROSECONDS.toNanos(1);
        }
    }

    private boolean finished() {
        return shouldStop && groups.isEmpty();
    }

    void doInDispatcherThread(FiberQueueTask r) {
        if (Thread.currentThread() == thread) {
            r.run();
        } else {
            shareQueue.offer(r);
        }
    }

    private int compareFiberByScheduleTime(Fiber f1, Fiber f2) {
        long diff = f1.scheduleNanoTime = f2.scheduleNanoTime;
        return diff < 0 ? -1 : diff > 0 ? 1 : 0;
    }
}
