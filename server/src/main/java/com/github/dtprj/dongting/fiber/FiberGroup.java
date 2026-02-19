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
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.IdentityHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public class FiberGroup {
    private static final DtLog log = DtLogs.getLogger(FiberGroup.class);
    public final ShareStatusSource shareStatusSource;
    public final String name;
    public final Dispatcher dispatcher;
    public final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
    public final FiberCondition shouldStopCondition;

    final IndexedQueue<Fiber> readyFibers = new IndexedQueue<>(64);
    final IndexedQueue<Fiber> readyFibersNextRound1 = new IndexedQueue<>(16);
    final IndexedQueue<Fiber> readyFibersNextRound2 = new IndexedQueue<>(16);
    private final IdentityHashMap<Fiber, Fiber> normalFibers = new IdentityHashMap<>(128);
    private final IdentityHashMap<Fiber, Fiber> daemonFibers = new IdentityHashMap<>(128);

    final FiberChannel<Runnable> sysChannel;

    boolean finished;
    boolean ready;

    private final GroupExecutor executor;
    Fiber currentFiber;

    public FiberGroup(String name, Dispatcher dispatcher, ShareStatusSource sss) {
        this.name = name;
        this.dispatcher = dispatcher;
        this.sysChannel = new FiberChannel<>(this);
        this.executor = new GroupExecutor(this);
        this.shouldStopCondition = newCondition(name + "-shouldStop");
        this.shareStatusSource = sss;
    }

    public FiberGroup(String name, Dispatcher dispatcher) {
        this(name, dispatcher, new ShareStatusSource());
    }

    /**
     * can call in any thread
     */
    public boolean fireFiber(Fiber fiber) {
        if (fiber.group != this) {
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
        if (Thread.currentThread() == dispatcher.thread && dispatcher.thread.currentGroup == this) {
            requestShutdown0();
        } else {
            Fiber shutdownGroupFiber = new Fiber("shutdownGroup", this, new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    requestShutdown0();
                    return Fiber.frameReturn();
                }
            });
            fireFiber(shutdownGroupFiber);
        }
    }

    private void requestShutdown0() {
        // if the dispatcher stopped, no ops
        if (shareStatusSource.shouldStop) {
            return;
        }
        log.info("request shutdown group: {}", name);
        shareStatusSource.shouldStop = true;
        shareStatusSource.copy(true);
        shouldStopCondition.signalAll();
    }

    public static FiberGroup currentGroup() {
        return DispatcherThread.currentGroup();
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
        if (f.group.finished) {
            log.warn("group finished, ignore fiber start: {}", f.name);
            return;
        }
        if (f.started) {
            BugLog.log("fiber already started: {}", f.name);
            return;
        }
        f.started = true;
        if (f.daemon) {
            daemonFibers.put(f, f);
        } else {
            normalFibers.put(f, f);
        }
        tryMakeFiberReady(f, addFirst);
    }

    void removeFiber(Fiber f) {
        boolean removed;
        if (f.daemon) {
            removed = daemonFibers.remove(f) != null;
        } else {
            removed = normalFibers.remove(f) != null;
        }
        if (!removed) {
            BugLog.log("fiber is not in set: {}", f.name);
        }
    }

    void tryMakeFiberReady(Fiber f, boolean addFirst) {
        if (finished) {
            log.warn("group finished, ignore makeReady: {}", f.name);
            return;
        }
        if (f.finished) {
            log.warn("fiber already finished, ignore makeReady: {}", f.name);
            return;
        }
        if (!f.ready) {
            f.ready = true;
            if ((f.roundInfo >>> 16) != (dispatcher.round & 0x00FF)) {
                f.roundInfo = (dispatcher.round << 16) | f.signalCountInEachRound;
            }
            if ((f.roundInfo & 0x00FF) == 0) {
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
                f.roundInfo--;
            }
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
        boolean ss = shareStatusSource.shouldStop;
        if (ss && !finished) {
            if (!normalFibers.isEmpty()) {
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

    boolean isShouldStopPlain() {
        return shareStatusSource.shouldStop;
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
                sb.append(", waitOn=").append(f.source).append(", timeout=").append(f.scheduleTimeout/1000/1000);
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
        sb.append(f.name).append("(").append(Integer.toHexString(f.hashCode())).append(")");
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
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

