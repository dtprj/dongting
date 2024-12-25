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

import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.buf.PoolFactory;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.buf.TwoLevelPool;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.NoopPerfCallback;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.PerfConsts;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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

    private final PoolFactory poolFactory;
    final PerfCallback perfCallback;

    final FiberQueue shareQueue = new FiberQueue();
    private final ArrayList<FiberGroup> groups = new ArrayList<>();
    private final ArrayList<FiberGroup> finishedGroups = new ArrayList<>();
    final IndexedQueue<FiberGroup> readyGroups = new IndexedQueue<>(8);
    private final PriorityQueue<Fiber> scheduleQueue = new PriorityQueue<>(this::compareFiberByScheduleTime);

    final Timestamp ts = new Timestamp();

    final DispatcherThread thread;

    private boolean poll = true;
    private long pollTimeout = TimeUnit.MILLISECONDS.toNanos(50);

    @SuppressWarnings("FieldMayBeFinal")
    private volatile boolean shouldStop = false;
    private final static VarHandle SHOULD_STOP;

    private long lastCleanNanos;

    int round;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            SHOULD_STOP = l.findVarHandle(Dispatcher.class, "shouldStop", boolean.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    // fatal error will cause fiber exit
    private Throwable fatalError;

    private DtTime stopTimeout;

    public Dispatcher(String name) {
        this(name, new DefaultPoolFactory(), NoopPerfCallback.INSTANCE);
    }

    public Dispatcher(String name, PoolFactory poolFactory, PerfCallback perfCallback) {
        this.thread = new DispatcherThread(this::run, name);
        this.poolFactory = poolFactory;
        this.perfCallback = perfCallback;

        this.thread.setHeapPool(createHeapPoolFactory());
        this.thread.setDirectPool(poolFactory.createPool(ts, true));
    }

    private RefBufferFactory createHeapPoolFactory() {
        TwoLevelPool heapPool = (TwoLevelPool) poolFactory.createPool(ts, false);
        TwoLevelPool releaseSafePool = heapPool.toReleaseInOtherThreadInstance(thread, byteBuffer -> {
            if (byteBuffer != null) {
                shareQueue.offer(new FiberQueueTask(null) {
                    @Override
                    protected void run() {
                        heapPool.getSmallPool().release(byteBuffer);
                    }
                });
            }
        });
        return new RefBufferFactory(releaseSafePool, 800);
    }

    public CompletableFuture<Void> startGroup(FiberGroup fiberGroup) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        boolean b = shareQueue.offer(new FiberQueueTask(fiberGroup) {
            @Override
            protected void run() {
                groups.add(fiberGroup);
                fiberGroup.startGroupRunnerFiber();
                future.complete(null);
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
        shareQueue.offer(new FiberQueueTask(null) {
            @Override
            protected void run() {
                SHOULD_STOP.setVolatile(Dispatcher.this, true);
                stopTimeout = timeout;
                groups.forEach(FiberGroup::requestShutdown);
            }
        });
    }

    private void run() {
        try {
            ArrayList<FiberQueueTask> localData = new ArrayList<>(64);
            ts.refresh();
            while (!isShouldStopPlain() || !groups.isEmpty()) {
                runImpl(localData);
                if (isShouldStopPlain() && stopTimeout != null && stopTimeout.isTimeout(ts)) {
                    long millis = stopTimeout.getTimeout(TimeUnit.MILLISECONDS);
                    log.warn("dispatcher stop timeout: {}ms", millis);
                    for (FiberGroup g : groups) {
                        g.fireLogGroupInfo("group not finished in " + millis + "ms");
                    }
                    stopTimeout = null;
                }
                round++;
            }
            shareQueue.shutdown();
            runImpl(localData);
            log.info("fiber dispatcher exit: {}", thread.getName());
        } catch (Throwable e) {
            SHOULD_STOP.setVolatile(this, true);
            log.info("fiber dispatcher exit exceptionally: {}", thread.getName(), e);
        } finally {
            poolFactory.destroyPool(thread.getHeapPool().getPool());
            poolFactory.destroyPool(thread.getDirectPool());
        }
    }

    private void runImpl(ArrayList<FiberQueueTask> localData) {
        fill(ts, localData);
        PerfCallback c = perfCallback;
        long start = c.takeTime(PerfConsts.FIBER_D_WORK, ts);
        processScheduleFibers();
        for (int len = localData.size(), i = 0; i < len; i++) {
            try {
                FiberQueueTask r = localData.get(i);
                r.run();
            } catch (Throwable e) {
                log.error("dispatcher run task fail", e);
            }
        }
        localData.clear();

        for (int len = readyGroups.size(), i = 0; i < len; i++) {
            FiberGroup g = readyGroups.removeFirst();
            execGroup(g);
            if (g.ready) {
                readyGroups.addLast(g);
            }
        }
        if (!finishedGroups.isEmpty()) {
            groups.removeAll(finishedGroups);
        }

        // 60 seconds clean once
        cleanPool(60_000_000_000L);

        if (c.accept(PerfConsts.FIBER_D_WORK) || c.accept(PerfConsts.FIBER_D_POLL)) {
            perfCallback.refresh(ts);
        } else {
            ts.refresh(1);
        }
        perfCallback.fireTime(PerfConsts.FIBER_D_WORK, start, 1, 0, ts);
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
                    BugLog.getLog().error("group finished, but suspend fiber is not daemon: {}", f.getName());
                }
                continue;
            }
            if (f.source != null) {
                f.source.removeWaiter(f);
                f.source.prepare(f, true);
            }
            f.cleanSchedule();
            f.fiberGroup.tryMakeFiberReady(f, false);
        }
    }

    private void cleanPool(long timeoutNanos) {
        if (ts.getNanoTime() - lastCleanNanos > timeoutNanos) {
            lastCleanNanos = ts.getNanoTime();
            thread.getHeapPool().getPool().clean();
            thread.getDirectPool().clean();
        }
    }

    private void execGroup(FiberGroup g) {
        thread.currentGroup = g;
        try {
            IndexedQueue<Fiber> readyQueue = g.readyFibers;
            IndexedQueue<Fiber> nextQueue1 = g.readyFibersNextRound1;
            IndexedQueue<Fiber> nextQueue2 = g.readyFibersNextRound2;
            if (nextQueue1.size() > 0) {
                for (int i = 0, size = nextQueue1.size(); i < size; i++) {
                    Fiber f = nextQueue1.removeFirst();
                    readyQueue.addFirst(f);
                }
            }
            if (nextQueue2.size() > 0) {
                for (int i = 0, size = nextQueue2.size(); i < size; i++) {
                    Fiber f = nextQueue2.removeFirst();
                    readyQueue.addLast(f);
                }
            }
            Fiber fiber = readyQueue.removeFirst();
            while (fiber != null) {
                execFiber(g, fiber);
                fiber = readyQueue.removeFirst();
            }

            g.updateFinishStatus();
            if (g.finished) {
                log.info("fiber group finished: {}", g.getName());
                finishedGroups.add(g);
                g.ready = false;
            } else {
                g.ready = readyQueue.size() > 0 || nextQueue1.size() > 0 || nextQueue2.size() > 0;
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
                execFrame(fiber, currentFrame);
                if (fatalError != null) {
                    break;
                }
                if (!fiber.ready) {
                    if (fiber.source == null && fiber.scheduleTimeoutMillis == 0) {
                        // yield
                        fiber.cleanSchedule();
                        fiber.ready = true;
                        fiber.fiberGroup.readyFibersNextRound2.addLast(fiber);
                    }
                    return;
                }
                //noinspection StatementWithEmptyBody
                if (currentFrame == fiber.stackTop) {
                    if (currentFrame.resumePoint != null) {
                        // call Fiber.resume
                        continue;
                    }
                    fiber.inputObj = currentFrame.frameResult;
                    fiber.inputEx = currentFrame.frameEx;
                    fiber.popFrame(); // remove self
                } else {
                    // call new frame
                }
                currentFrame = fiber.stackTop;
            }
            if (fatalError != null) {
                log.error("fiber execute error, group={}, fiber={}", g.getName(), fiber.getName(), fatalError);
            }
            if (fiber.inputEx != null) {
                log.error("fiber execute error, group={}, fiber={}", g.getName(), fiber.getName(), fiber.inputEx);
            }
            fiber.finished = true;
            fiber.ready = false;
            g.removeFiber(fiber);
            fiber.signalAll0(false);
        } finally {
            g.currentFiber = null;
            fatalError = null;
        }
    }

    private void execFrame(Fiber fiber, FiberFrame currentFrame) {
        try {
            if (fiber.inputEx != null) {
                // ex throws by sub-frame, or after this frame suspend
                Throwable x = fiber.inputEx;
                fiber.inputEx = null;
                fiber.inputObj = null;
                tryHandleEx(currentFrame, x);
            } else {
                try {
                    Object input = fiber.inputObj;
                    fiber.inputObj = null;
                    FrameCall r = currentFrame.resumePoint;
                    currentFrame.resumePoint = null;
                    r.execute(input);
                    if (fiber.inputEx != null) {
                        // Fiber.resumeEx() called
                        throw fiber.inputEx;
                    }
                } catch (FiberException e) {
                    tryHandleEx(currentFrame, e);
                } catch (Throwable e) {
                    FiberException.addVirtualStackTrace(e, fiber);
                    tryHandleEx(currentFrame, e);
                }
            }
        } finally {
            try {
                if (!currentFrame.finallyCalled && currentFrame.resumePoint == null) {
                    currentFrame.catchCalled = true;
                    currentFrame.finallyCalled = true;
                    currentFrame.doFinally();
                }
            } catch (FiberException e) {
                setEx(currentFrame, e);
            } catch (Throwable e) {
                FiberException.addVirtualStackTrace(e, fiber);
                setEx(currentFrame, e);
            }
        }
    }

    void setEx(FiberFrame currentFrame, Throwable ex) {
        if (currentFrame.frameEx != null) {
            ex.addSuppressed(currentFrame.frameEx);
            if (!currentFrame.finallyCalled) {
                BugLog.getLog().error("usage fatal error: throw ex after fiber suspend", ex);
                fatalError = ex;
                return;
            }
        }
        currentFrame.frameEx = ex;
    }

    private void tryHandleEx(FiberFrame currentFrame, Throwable x) {
        currentFrame.resumePoint = null;
        if (!currentFrame.catchCalled) {
            currentFrame.catchCalled = true;
            try {
                currentFrame.handle(x);
            } catch (FiberException e) {
                currentFrame.frameEx = e;
            } catch (Throwable e) {
                if (e != x) {
                    FiberException.addVirtualStackTrace(e, currentFrame.fiber);
                }
                currentFrame.frameEx = e;
            }
        } else {
            setEx(currentFrame, x);
        }
    }

    static void call(FiberFrame subFrame, FrameCall resumePoint) {
        Fiber fiber = getCurrentFiberAndCheck(null);
        checkReentry(fiber);
        FiberFrame currentFrame = fiber.stackTop;
        currentFrame.resumePoint = resumePoint;
        subFrame.init(fiber);
        fiber.pushFrame(subFrame);
    }

    static <O> void resume(O input, Throwable ex, FrameCall<O> resumePoint) {
        Fiber fiber = getCurrentFiberAndCheck(null);
        checkReentry(fiber);
        FiberFrame currentFrame = fiber.stackTop;
        fiber.inputObj = input;
        fiber.inputEx = ex;
        currentFrame.resumePoint = resumePoint;
    }

    static FrameCallResult awaitOn(WaitSource c, long millis, FrameCall resumePoint) {
        Fiber fiber = getCurrentFiberAndCheck(c.fiberGroup);
        return awaitOn(fiber, c, millis, resumePoint);
    }

    static FrameCallResult awaitOn(Fiber fiber, WaitSource c, long millis, FrameCall resumePoint) {
        checkInterrupt(fiber);
        checkReentry(fiber);
        FiberFrame currentFrame = fiber.stackTop;
        currentFrame.resumePoint = resumePoint;
        fiber.source = c;
        fiber.scheduleTimeoutMillis = millis;
        fiber.ready = false;
        fiber.fiberGroup.dispatcher.addToScheduleQueue(millis, fiber);
        c.addWaiter(fiber);
        return FrameCallResult.SUSPEND;
    }

    static void sleep(long millis, FrameCall<Void> resumePoint) {
        Fiber fiber = getCurrentFiberAndCheck(null);
        checkInterrupt(fiber);
        checkReentry(fiber);
        FiberFrame currentFrame = fiber.stackTop;
        currentFrame.resumePoint = resumePoint;
        fiber.scheduleTimeoutMillis = millis;
        fiber.ready = false;
        fiber.fiberGroup.dispatcher.addToScheduleQueue(millis, fiber);
    }

    static void sleepUntilShouldStop(long millis, FrameCall<Void> resumePoint) {
        Fiber fiber = getCurrentFiberAndCheck(null);
        checkInterrupt(fiber);
        checkReentry(fiber);
        FiberGroup g = fiber.fiberGroup;
        if (g.isShouldStopPlain()) {
            // as same as yield, to avoid dead loop if the resumePoint not check shouldStop and call this method again
            FiberFrame currentFrame = fiber.stackTop;
            currentFrame.resumePoint = resumePoint;
            fiber.ready = false;
            return;
        }
        FiberFrame currentFrame = fiber.stackTop;
        currentFrame.resumePoint = resumePoint;
        fiber.scheduleTimeoutMillis = millis;
        fiber.source = g.shouldStopCondition;
        g.shouldStopCondition.addWaiter(fiber);
        fiber.ready = false;
        fiber.fiberGroup.dispatcher.addToScheduleQueue(millis, fiber);
    }

    static void yield(FrameCall<Void> resumePoint) {
        Fiber fiber = getCurrentFiberAndCheck(null);
        checkReentry(fiber);
        FiberFrame currentFrame = fiber.stackTop;
        currentFrame.resumePoint = resumePoint;
        fiber.ready = false;
    }

    private void addToScheduleQueue(long millis, Fiber fiber) {
        if (millis > 0) {
            fiber.scheduleNanoTime = ts.getNanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
            scheduleQueue.add(fiber);
        }
    }

    static Fiber getCurrentFiberAndCheck(FiberGroup expectGroup) {
        DispatcherThread dispatcherThread = DispatcherThread.currentDispatcherThread();
        FiberGroup dispatcherGroup = dispatcherThread.currentGroup;
        Fiber fiber = dispatcherGroup.currentFiber;
        if (fiber == null) {
            throwFatalError(dispatcherGroup, "usage fatal error: current fiber is null");
        }
        if (expectGroup != null && dispatcherGroup != expectGroup) {
            throw new FiberException("current fiber group not match");
        }
        //noinspection DataFlowIssue
        if (!fiber.ready) {
            throwFatalError(dispatcherGroup, "usage fatal error: current fiber not ready state");
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

    void removeFromScheduleQueue(Fiber f) {
        scheduleQueue.remove(f);
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
                str = s.toString();
            } else {
                str = "sleep";
            }
            fiber.stackTop.resumePoint = null;
            fiber.interrupted = false;
            fiber.inputEx = new FiberInterruptException("fiber is interrupted during wait " + str);
            if (fiber.scheduleTimeoutMillis > 0) {
                scheduleQueue.remove(fiber);
                fiber.cleanSchedule();
            }
            fiber.fiberGroup.tryMakeFiberReady(fiber, false);
        }
    }

    private void fill(Timestamp ts, ArrayList<FiberQueueTask> localData) {
        try {
            long oldNanos = ts.getNanoTime();

            if (!poll || readyGroups.size() > 0) {
                shareQueue.drainTo(localData);
            } else {
                Fiber f = scheduleQueue.peek();
                long t;
                if (f != null) {
                    t = Math.min(f.scheduleNanoTime - oldNanos, pollTimeout);
                } else {
                    t = pollTimeout;
                }
                if (t > 0) {
                    PerfCallback c = perfCallback;
                    long startTime = c.takeTime(PerfConsts.FIBER_D_POLL, ts);
                    // 100ms clean once
                    cleanPool(100_000_000L);
                    FiberQueueTask o = shareQueue.poll(t, TimeUnit.NANOSECONDS);
                    if (c.accept(PerfConsts.FIBER_D_WORK) || c.accept(PerfConsts.FIBER_D_POLL)) {
                        perfCallback.refresh(ts);
                    } else {
                        ts.refresh(1);
                    }
                    c.fireTime(PerfConsts.FIBER_D_POLL, startTime, 1, 0, ts);
                    if (o != null) {
                        localData.add(o);
                    }
                } else {
                    shareQueue.drainTo(localData);
                }
            }

            poll = ts.getNanoTime() - oldNanos > 2_000_000 || localData.isEmpty();
        } catch (InterruptedException e) {
            log.info("fiber dispatcher receive interrupt signal");
            pollTimeout = TimeUnit.MICROSECONDS.toNanos(1);
        }
    }

    boolean doInDispatcherThread(FiberQueueTask r) {
        if (Thread.currentThread() == thread) {
            if (r.ownerGroup != null && r.ownerGroup.finished) {
                log.warn("task is not accepted because its group is finished: {}", r);
                return false;
            } else {
                r.run();
                return true;
            }
        } else {
            return shareQueue.offer(r);
        }
    }

    private int compareFiberByScheduleTime(Fiber f1, Fiber f2) {
        long diff = f1.scheduleNanoTime - f2.scheduleNanoTime;
        return diff < 0 ? -1 : diff > 0 ? 1 : 0;
    }

    public Timestamp getTs() {
        return ts;
    }

    private boolean isShouldStopPlain() {
        return (boolean) SHOULD_STOP.get(this);
    }

    public DispatcherThread getThread() {
        return thread;
    }
}
