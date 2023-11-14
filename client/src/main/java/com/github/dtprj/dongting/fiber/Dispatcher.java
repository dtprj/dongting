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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Dispatcher extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(Dispatcher.class);

    final LinkedBlockingQueue<Runnable> shareQueue = new LinkedBlockingQueue<>();
    private final ArrayList<FiberGroup> groups = new ArrayList<>();
    private final ArrayList<FiberGroup> finishedGroups = new ArrayList<>();
    final IndexedQueue<FiberGroup> readyGroups = new IndexedQueue<>(8);
    private final PriorityQueue<Fiber> scheduleQueue = new PriorityQueue<>(this::compareFiberByScheduleTime);

    private final Timestamp ts = new Timestamp();

    private Fiber currentFiber;
    final Thread thread;

    private boolean poll = true;
    private long pollTimeout = TimeUnit.MILLISECONDS.toNanos(50);

    private boolean shouldStop = false;

    Object inputObj;
    private Throwable fatalError;

    public Dispatcher(String name) {
        thread = new Thread(this::run, name);
    }

    public CompletableFuture<FiberGroup> createFiberGroup(String name) {
        CompletableFuture<FiberGroup> future = new CompletableFuture<>();
        FiberGroup g = new FiberGroup(name, this);
        shareQueue.offer(() -> {
            if (shouldStop) {
                future.completeExceptionally(new FiberException("fiber group already stopped"));
            } else {
                groups.add(g);
                future.complete(g);
            }
        });
        return future;
    }

    @Override
    protected void doStart() {
        thread.start();
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
        shareQueue.offer(() -> {
            shouldStop = true;
            groups.forEach(g -> g.shouldStop = true);
        });
    }

    private void run() {
        ArrayList<Runnable> localData = new ArrayList<>(64);
        while (!finished()) {
            pollAndRefreshTs(ts, localData);
            processScheduleFibers();
            int len = localData.size();
            for (int i = 0; i < len; i++) {
                Runnable r = localData.get(i);
                r.run();
            }

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
        log.info("fiber dispatcher exit: {}", thread.getName());
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
            if (f.source != null) {
                f.lastEx = new FiberTimeoutException(f.scheduleTimeoutMillis + "ms");
                f.source.removeWaiter(f);
                f.source = null;
            }
            f.scheduleTimeoutMillis = 0;
            f.scheduleNanoTime = 0;
            f.fiberGroup.tryMakeFiberReady(f, true);
        }
    }

    private void execGroup(FiberGroup g) {
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
    }

    private void execFiber(FiberGroup g, Fiber fiber) {
        try {
            currentFiber = fiber;
            FiberFrame currentFrame = fiber.stackTop;
            while (currentFrame != null) {
                if (fiber.source != null) {
                    if (fiber.source instanceof FiberFuture) {
                        FiberFuture fu = (FiberFuture) fiber.source;
                        fiber.lastEx = fu.execEx;
                        inputObj = fu.result;
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
                        // called awaitOn() on a completed future
                        if (fiber.lastEx == null) {
                            continue;
                        } else {
                            // user code throws ex after called awaitOn
                            BugLog.getLog().error("usage fatal error: throw ex after called awaitOn", fiber.lastEx);
                            break;
                        }
                    }
                    inputObj = currentFrame.result;
                    currentFrame = fiber.popFrame();
                } else {
                    // call new frame
                    if (fiber.lastEx != null) {
                        fiber.lastEx = new FiberException(
                                "usage fatal error: suspendCall() should be last statement", fiber.lastEx);
                        break;
                    }
                    currentFrame = fiber.stackTop;
                }
            }
            if (fiber.lastEx != null) {
                log.error("fiber execute error, group={}, fiber={}", g.getName(), fiber.getFiberName(), fiber.lastEx);
            }
            fiber.finished = true;
            fiber.ready = false;
            g.removeFiber(fiber);
        } finally {
            inputObj = null;
            currentFiber = null;
            fiber.lastEx = null;
            fatalError = null;
        }
    }

    private void processFrame(Fiber fiber, FiberFrame currentFrame) {
        try {
            if (fiber.lastEx != null) {
                currentFrame.bodyFinished = true;
                currentFrame.resumePoint = null;
                tryHandleEx(currentFrame, fiber.lastEx);
            } else {
                try {
                    FrameCall r = currentFrame.resumePoint;
                    FrameCallResult result;
                    Object input = inputObj;
                    inputObj = null;
                    if (r == null) {
                        result = currentFrame.execute(input);
                    } else {
                        currentFrame.resumePoint = null;
                        result = r.execute(input);
                    }
                    if (result == FrameCallResult.RETURN) {
                        currentFrame.bodyFinished = true;
                    }
                    checkResult(result, fiber);
                } catch (Throwable e) {
                    currentFrame.bodyFinished = true;
                    if (!tryHandleEx(currentFrame, e)) {
                        fiber.lastEx = e;
                    }
                }
            }
        } finally {
            try {
                if (currentFrame.bodyFinished && !currentFrame.finallyCalled) {
                    currentFrame.finallyCalled = true;
                    FrameCallResult result = currentFrame.doFinally();
                    checkResult(result, fiber);
                }
            } catch (Throwable e) {
                fiber.lastEx = e;
            }
        }
    }

    private boolean tryHandleEx(FiberFrame currentFrame, Throwable x) {
        if (!currentFrame.handleCalled && currentFrame instanceof FiberFrameEx) {
            currentFrame.handleCalled = true;
            currentFrame.fiber.lastEx = null;
            try {
                FrameCallResult result = ((FiberFrameEx) currentFrame).handle(x);
                checkResult(result, currentFrame.fiber);
            } catch (Throwable e) {
                currentFrame.fiber.lastEx = e;
            }
            return true;
        }
        return false;
    }

    void call(Object input, FiberFrame currentFrame, FiberFrame subFrame, FrameCall resumePoint) {
        checkCurrentFrame(currentFrame);
        currentFrame.resumePoint = resumePoint;
        subFrame.fiberGroup = currentFrame.fiberGroup;
        subFrame.fiber = currentFrame.fiber;
        inputObj = input;
        currentFiber.pushFrame(subFrame);
    }

    void awaitOn(FiberFrame currentFrame, WaitSource c, long millis, FrameCall resumePoint) {
        checkCurrentFrame(currentFrame);
        currentFrame.resumePoint = resumePoint;
        Fiber fiber = currentFrame.fiber;
        fiber.source = c;
        if (c instanceof FiberFuture) {
            FiberFuture fu = (FiberFuture) c;
            if (fu.isDone()) {
                return;
            }
        }
        fiber.ready = false;
        if (millis > 0) {
            addToScheduleQueue(millis, fiber);
        }
        c.addWaiter(fiber);
    }

    void sleep(FiberFrame currentFrame, long millis, FrameCall resumePoint) {
        checkCurrentFrame(currentFrame);
        currentFrame.resumePoint = resumePoint;
        Fiber fiber = currentFrame.fiber;
        fiber.ready = false;
        addToScheduleQueue(millis, fiber);
    }

    private void addToScheduleQueue(long millis, Fiber fiber) {
        fiber.scheduleTimeoutMillis = millis;
        fiber.scheduleNanoTime = ts.getNanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        scheduleQueue.add(fiber);
    }

    private void checkCurrentFrame(FiberFrame current) {
        if (current.resumePoint != null) {
            throwFatalError("usage fatal error: already suspended");
        }
        Fiber fiber = current.fiber;
        if (fiber != currentFiber) {
            throwFatalError("usage fatal error: fiber not match");
        }
        if (fiber.interrupted) {
            fiber.interrupted = false;
            throw new FiberInterruptException("fiber is interrupted");
        }
        if (!fiber.ready) {
            throwFatalError("usage fatal error: fiber not ready state");
        }
        if (fiber.stackTop != current) {
            throwFatalError("usage fatal error: can't call suspendCall twice");
        }
    }

    void throwFatalError(String msg) {
        FiberException fe = new FiberException(msg);
        fatalError = fe;
        throw fe;
    }

    void checkResult(FrameCallResult r, Fiber f) {
        if (r == FrameCallResult.CALL_NEXT_FRAME) {
            if (!f.ready) {
                throwFatalError("usage fatal error: fiber not ready");
            }
        }
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
        if (fiber.ready) {
            fiber.interrupted = true;
        } else {
            if (fiber.source != null) {
                fiber.source.removeWaiter(fiber);
                fiber.source = null;
            }
            fiber.interrupted = false;
            fiber.lastEx = new FiberInterruptException("fiber is interrupted during wait");
            tryRemoveFromScheduleQueue(fiber);
            fiber.fiberGroup.tryMakeFiberReady(fiber, false);
        }
    }

    private void pollAndRefreshTs(Timestamp ts, ArrayList<Runnable> localData) {
        try {
            long oldNanos = ts.getNanoTime();
            Fiber f = scheduleQueue.peek();
            long t = 0;
            if (f != null) {
                t = f.scheduleNanoTime - oldNanos;
            }
            if (poll && t > 0) {
                Runnable o = shareQueue.poll(Math.min(t, pollTimeout), TimeUnit.NANOSECONDS);
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

    void doInDispatcherThread(Runnable r) {
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
