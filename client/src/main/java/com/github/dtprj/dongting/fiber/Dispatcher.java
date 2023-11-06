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

import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class Dispatcher extends Thread {
    private static final DtLog log = DtLogs.getLogger(Dispatcher.class);

    private final LinkedBlockingQueue<Runnable> shareQueue = new LinkedBlockingQueue<>();
    private final ArrayList<FiberGroup> groups = new ArrayList<>();
    private final IndexedQueue<Integer> finishedGroups = new IndexedQueue<>(8);

    private final Timestamp ts = new Timestamp();

    private Fiber currentFiber;
    private Thread thread;

    private boolean poll = true;
    private int pollTimeout = 50;

    private boolean shouldStop = false;

    Object lastResultObj;
    int lastResultInt;
    long lastResultLong;
    Throwable fatalError;
    Throwable lastEx;

    public Dispatcher(String name) {
        super(name);
    }

    public CompletableFuture<FiberGroup> createFiberGroup(String name) {
        CompletableFuture<FiberGroup> future = new CompletableFuture<>();
        FiberGroup g = new FiberGroup(name, this);
        shareQueue.offer(() -> {
            if (g.isShouldStop()) {
                future.completeExceptionally(new FiberException("fiber group already stopped"));
            } else {
                groups.add(g);
                future.complete(g);
            }
        });
        return future;
    }

    public void requestShutdown() {
        shareQueue.offer(() -> {
            shouldStop = true;
            groups.forEach(g -> g.setShouldStop());
        });
    }

    @Override
    public void run() {
        this.thread = Thread.currentThread();
        ArrayList<Runnable> localData = new ArrayList<>(64);
        ArrayList<FiberGroup> groups = this.groups;
        while (!finished()) {
            pollAndRefreshTs(ts, localData);
            int len = localData.size();
            for (int i = 0; i < len; i++) {
                Runnable r = localData.get(i);
                r.run();
            }
            len = groups.size();
            for (int i = 0; i < len; i++) {
                FiberGroup g = groups.get(i);
                IndexedQueue<Fiber> readyQueue = g.getReadyQueue();
                while (readyQueue.size() > 0) {
                    Fiber fiber = readyQueue.removeFirst();
                    execFiber(g, fiber);
                }
                if (g.finished()) {
                    log.info("fiber group finished: {}", g.getName());
                    finishedGroups.addLast(i);
                }
            }
            while (finishedGroups.size() > 0) {
                int idx = finishedGroups.removeFirst();
                groups.remove(idx);
            }
        }
        log.info("fiber dispatcher exit: {}", getName());
    }

    private void execFiber(FiberGroup g, Fiber fiber) {
        try {
            currentFiber = fiber;
            FiberFrame currentFrame = fiber.stackTop;
            if (fiber.source instanceof FiberFuture) {
                FiberFuture fu = (FiberFuture) fiber.source;
                lastEx = fu.execEx;
                lastResultObj = fu.resultObj;
                lastResultInt = fu.resultInt;
                lastResultLong = fu.resultLong;
                fiber.source = null;
            }
            while (currentFrame != null) {
                process(fiber, currentFrame);
                if (fatalError != null) {
                    lastEx = fatalError;
                    break;
                }
                if (!fiber.ready) {
                    return;
                }
                if (currentFrame == fiber.stackTop) {
                    FiberFrame oldFrame = currentFrame;
                    currentFrame = fiber.popFrame();
                    if (currentFrame != null && lastEx == null) {
                        lastResultObj = oldFrame.outputObj;
                        lastResultInt = oldFrame.outputInt;
                        lastResultLong = oldFrame.outputLong;
                    } else {
                        lastResultObj = null;
                        lastResultInt = 0;
                        lastResultLong = 0;
                    }
                } else {
                    // call new frame
                    if (lastEx != null) {
                        lastEx = new FiberException("usage fatal error: suspendCall() should be last statement", lastEx);
                        break;
                    }
                    lastResultObj = null;
                    lastResultInt = 0;
                    lastResultLong = 0;
                    currentFrame = fiber.stackTop;
                }
            }
            if (lastEx != null) {
                log.error("fiber execute error, group={}, fiber={}", g.getName(), fiber.getFiberName(), lastEx);
            }
            fiber.finished = true;
            g.removeFiber(fiber);
        } finally {
            lastResultObj = null;
            lastResultInt = 0;
            lastResultLong = 0;
            currentFiber = null;
            lastEx = null;
            fatalError = null;
        }
    }

    private void process(Fiber fiber, FiberFrame currentFrame) {
        try {
            if (lastEx != null) {
                currentFrame.bodyFinished = true;
                currentFrame.resumePoint = null;
                tryHandleEx(currentFrame);
            } else {
                try {
                    Runnable r = currentFrame.resumePoint;
                    if (r == null) {
                        currentFrame.execute();
                    } else {
                        currentFrame.resumePoint = null;
                        r.run();
                    }
                    if (fiber.ready) {
                        currentFrame.bodyFinished = true;
                    }
                } catch (Throwable e) {
                    currentFrame.bodyFinished = true;
                    if (!tryHandleEx(currentFrame)) {
                        lastEx = e;
                    }
                }
            }
        } finally {
            try {
                if (currentFrame.bodyFinished && !currentFrame.finallyCalled) {
                    currentFrame.finallyCalled = true;
                    currentFrame.doFinally();
                }
            } catch (Throwable e) {
                lastEx = e;
            }
        }
    }

    private boolean tryHandleEx(FiberFrame currentFrame) {
        if (!currentFrame.handleCalled && currentFrame instanceof FiberFrameEx) {
            currentFrame.handleCalled = true;
            Throwable x = lastEx;
            lastEx = null;
            try {
                ((FiberFrameEx) currentFrame).handle(x);
            } catch (Throwable e) {
                lastEx = e;
            }
            return true;
        }
        return false;
    }

    void suspendCall(FiberFrame currentFrame, FiberFrame newFrame, Runnable resumePoint) {
        checkCurrentFrame(currentFrame);
        currentFrame.resumePoint = resumePoint;
        newFrame.group = currentFrame.group;
        newFrame.fiber = currentFrame.fiber;
        currentFiber.pushFrame(newFrame);
    }

    void awaitOn(FiberFrame currentFrame, WaitSource c, Runnable resumePoint) {
        checkCurrentFrame(currentFrame);
        currentFrame.resumePoint = resumePoint;
        Fiber fiber = currentFrame.fiber;
        fiber.ready = false;
        fiber.source = c;
        c.addWaiter(fiber);
    }

    private void checkCurrentFrame(FiberFrame current) {
        if (current.resumePoint != null) {
            throwFatalError("usage fatal error: already suspended");
        }
        Fiber fiber = current.fiber;
        if (fiber != currentFiber) {
            throwFatalError("usage fatal error: fiber not match");
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

    private void pollAndRefreshTs(Timestamp ts, ArrayList<Runnable> localData) {
        try {
            long oldNanos = ts.getNanoTime();
            if (poll) {
                Runnable o = shareQueue.poll(pollTimeout, TimeUnit.MILLISECONDS);
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
            pollTimeout = 1;
        }
    }

    private boolean finished() {
        return shouldStop && groups.isEmpty();
    }

    LinkedBlockingQueue<Runnable> getShareQueue() {
        return shareQueue;
    }

    Fiber getCurrentFiber() {
        return currentFiber;
    }

    void setCurrentFiber(Fiber currentFiber) {
        this.currentFiber = currentFiber;
    }

    Thread getThread() {
        return thread;
    }
}
