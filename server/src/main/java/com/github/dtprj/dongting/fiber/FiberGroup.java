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

import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public class FiberGroup {
    private static final DtLog log = DtLogs.getLogger(FiberGroup.class);
    private final String name;
    final Dispatcher dispatcher;
    final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();

    final IndexedQueue<Fiber> readyFibers = new IndexedQueue<>(64);
    final IndexedQueue<Fiber> readyFibersNextRound1 = new IndexedQueue<>(16);
    final IndexedQueue<Fiber> readyFibersNextRound2 = new IndexedQueue<>(16);
    private final LongObjMap<Fiber> normalFibers = new LongObjMap<>(32, 0.6f);
    private final LongObjMap<Fiber> daemonFibers = new LongObjMap<>(32, 0.6f);

    @SuppressWarnings("FieldMayBeFinal")
    private volatile boolean shouldStop = false;
    private final static VarHandle SHOULD_STOP;
    private long markStopNanos;

    final FiberChannel<Runnable> sysChannel;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            SHOULD_STOP = l.findVarHandle(FiberGroup.class, "shouldStop", boolean.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private long nextId;

    boolean finished;
    boolean ready;

    Fiber currentFiber;

    final FiberCondition shouldStopCondition;
    private final GroupExecutor executor;

    public FiberGroup(String name, Dispatcher dispatcher) {
        this.name = name;
        this.dispatcher = dispatcher;
        this.sysChannel = new FiberChannel<>(this);
        this.executor = new GroupExecutor(this);
        this.shouldStopCondition = newCondition(name + "-shouldStop");
    }

    /**
     * can call in any thread
     */
    public boolean fireFiber(Fiber fiber) {
        if (fiber.fiberGroup != this) {
            throw new DtException("fiber not in group");
        }
        return dispatcher.doInDispatcherThread(new FiberQueueTask(this) {
            @Override
            protected void run() {
                start(fiber, false);
            }
        });
    }

    /**
     * can call in any thread
     */
    public boolean fireFiber(String fiberName, FiberFrame<Void> firstFrame) {
        return fireFiber(new Fiber(fiberName, this, firstFrame));
    }

    /**
     * can call in any thread
     */
    public void requestShutdown() {
        Fiber shutdownGroupFiber = new Fiber("shutdownGroup", this, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                // if the dispatcher stopped, no ops
                if ((boolean) SHOULD_STOP.get(FiberGroup.this)) {
                    return Fiber.frameReturn();
                }
                log.info("request shutdown group: {}", name);
                markStopNanos = dispatcher.ts.getNanoTime();
                SHOULD_STOP.setVolatile(FiberGroup.this, true);
                shouldStopCondition.signalAll();
                return Fiber.frameReturn();
            }
        });
        fireFiber(shutdownGroupFiber);
    }

    public static FiberGroup currentGroup() {
        return DispatcherThread.currentGroup();
    }

    public String getName() {
        return name;
    }

    public FiberCondition newCondition(String name) {
        return new FiberCondition(name, this);
    }

    public <T> FiberFuture<T> newFuture(String name) {
        return new FiberFuture<>(name, this);
    }

    public <T> FiberChannel<T> newChannel() {
        return new FiberChannel<>(this);
    }

    public FiberLock newLock(String name) {
        return new FiberLock(name, this);
    }

    void checkGroup() {
        if (DispatcherThread.currentGroup() != this) {
            throw new FiberException("not in current group: " + name);
        }
    }

    void startGroupRunnerFiber() {
        GroupRunnerFiberFrame frame = new GroupRunnerFiberFrame(sysChannel);
        Fiber f = new Fiber("group-runner", this, frame, true, 100);
        start(f, false);
    }

    void start(Fiber f, boolean addFirst) {
        if (f.fiberGroup.finished) {
            log.warn("group finished, ignore fiber start: {}", f.getName());
            return;
        }
        if (f.started) {
            BugLog.getLog().error("fiber already started: {}", f.getName());
            return;
        }
        f.started = true;
        long id = nextId++;
        f.id = id;
        if (f.daemon) {
            daemonFibers.put(id, f);
        } else {
            normalFibers.put(id, f);
        }
        tryMakeFiberReady(f, addFirst);
    }

    void removeFiber(Fiber f) {
        boolean removed;
        if (f.daemon) {
            removed = daemonFibers.remove(f.id) != null;
        } else {
            removed = normalFibers.remove(f.id) != null;
        }
        if (!removed) {
            BugLog.getLog().error("fiber is not in set: {}", f.getName());
        }
    }

    void tryMakeFiberReady(Fiber f, boolean addFirst) {
        if (finished) {
            log.warn("group finished, ignore makeReady: {}", f.getName());
            return;
        }
        if (f.finished) {
            log.warn("fiber already finished, ignore makeReady: {}", f.getName());
            return;
        }
        if (!f.ready) {
            f.ready = true;
            if (f.round != dispatcher.round) {
                f.round = dispatcher.round;
                f.signalCountInCurrentRound = f.signalCountInEachRound;
            }
            if (f.signalCountInCurrentRound <= 0) {
                if (addFirst) {
                    readyFibersNextRound1.addLast(f);
                } else {
                    readyFibersNextRound2.addLast(f);
                }
            } else {
                if (addFirst) {
                    readyFibers.addFirst(f);
                } else {
                    readyFibers.addLast(f);
                }
            }
            f.signalCountInCurrentRound--;
            makeGroupReady();
        }
    }

    private void makeGroupReady() {
        if (ready) {
            return;
        }
        ready = true;
        dispatcher.readyGroups.addLast(this);
    }

    void updateFinishStatus() {
        boolean ss = (boolean) SHOULD_STOP.get(this);
        if (ss && !finished) {
            if (normalFibers.size() > 0) {
                return;
            }
            if (sysChannel.queue.size() > 0) {
                return;
            }
            // update finished status in lock, so that other threads can see it in this lock
            ReentrantLock lock = dispatcher.shareQueue.lock;
            lock.lock();
            try {
                if (!dispatcher.shareQueue.hasTask(this)) {
                    finished = true;
                }
            } finally {
                lock.unlock();
            }
            if (finished) {
                shutdownFuture.complete(null);
            }
        }
    }

    public boolean isShouldStop() {
        return (boolean) SHOULD_STOP.getOpaque(this);
    }

    boolean isShouldStopPlain() {
        return (boolean) SHOULD_STOP.get(this);
    }

    public void fireLogGroupInfo(String msg) {
        if (!log.isInfoEnabled()) {
            return;
        }
        if (Thread.currentThread() == dispatcher.thread) {
            logGroupInfo0(msg);
        } else {
            CompletableFuture<Void> f = new CompletableFuture<>();
            boolean b = dispatcher.doInDispatcherThread(new FiberQueueTask(this) {
                @Override
                protected void run() {
                    logGroupInfo0(msg);
                    f.complete(null);
                }
            });
            if (!b) {
                log.error("dispatcher or group is shutdown and can't log group info");
                return;
            }
            try {
                f.get(3, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                log.error("log group info timeout, group={}", name, e);
                try {
                    Exception stack = new Exception();
                    stack.setStackTrace(dispatcher.thread.getStackTrace());
                    log.error("dispatcher thread stack", stack);
                } catch (Exception ignored) {
                }
            } catch (Exception e) {
                log.error("can't log group info, group={}", name, e);
            }
        }
    }

    private void logGroupInfo0(String msg) {
        StringBuilder sb = new StringBuilder(256);
        sb.append(msg).append("\ngroup ").append(name)
                .append(", ready=").append(readyFibers.size())
                .append(", readyNext=").append(readyFibersNextRound1.size() + readyFibersNextRound2.size())
                .append(", normal=").append(normalFibers.size())
                .append(", daemon=").append(daemonFibers.size())
                .append("\n")
                .append("--------------------------------------------------\n")
                .append("readyFibers:\n");
        concatReadyFibers(readyFibers, sb);
        concatReadyFibers(readyFibersNextRound1, sb);
        concatReadyFibers(readyFibersNextRound2, sb);
        sb.append("--------------------------------------------------\n");
        sb.append("normalFibers:\n");
        normalFibers.forEach((key, f) -> {
            if (!f.ready) {
                concatFiberName(sb, f);
                sb.append(", waitOn=").append(f.source).append(", timeout=").append(f.scheduleTimeoutMillis);
                sb.append('\n');
            }
        });
        sb.append("--------------------------------------------------\n");
        log.info(sb.toString());
    }

    private void concatReadyFibers(IndexedQueue<Fiber> q, StringBuilder sb) {
        for (int i = 0; i < q.size(); i++) {
            Fiber f = q.get(i);
            concatFiberName(sb, f);
            sb.append(", currentFrame=").append(f.stackTop).append(", resumePoint=");
            if (f.stackTop == null) {
                sb.append("null");
            } else {
                sb.append(f.stackTop.resumePoint);
            }
            sb.append("\n");
        }
    }

    private void concatFiberName(StringBuilder sb, Fiber f) {
        sb.append(f.getName()).append("(").append(Integer.toHexString(f.hashCode())).append(")");
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public DispatcherThread getThread() {
        return dispatcher.thread;
    }

    public CompletableFuture<Void> getShutdownFuture() {
        return shutdownFuture;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    /**
     * should call in dispatcher thread.
     */
    public long timeAfterRequestStop(TimeUnit unit) {
        checkGroup();
        if (!isShouldStopPlain()) {
            return -1;
        }
        return unit.convert(dispatcher.ts.getNanoTime() - markStopNanos, TimeUnit.NANOSECONDS);
    }
}

class GroupRunnerFiberFrame extends FiberFrame<Void> {
    private static final DtLog log = DtLogs.getLogger(GroupRunnerFiberFrame.class);

    private final FiberChannel<Runnable> channel;

    public GroupRunnerFiberFrame(FiberChannel<Runnable> channel) {
        this.channel = channel;
    }

    @Override
    public FrameCallResult execute(Void input) {
        return channel.take(this::afterTake);
    }

    private FrameCallResult afterTake(Runnable r) {
        if (r != null) {
            try {
                r.run();
            } catch (Throwable e) {
                log.error("callback error", e);
            }
        }
        return Fiber.resume(null, this);
    }
}

