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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author huangli
 */
public class FiberGroup {
    private static final DtLog log = DtLogs.getLogger(FiberGroup.class);
    private final String name;
    final Dispatcher dispatcher;
    final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();

    final IndexedQueue<Fiber> readyFibers = new IndexedQueue<>(64);
    final IndexedQueue<Fiber> readyFibersNextRound = new IndexedQueue<>(16);
    private final LongObjMap<Fiber> normalFibers = new LongObjMap<>(32, 0.6f);
    private final LongObjMap<Fiber> daemonFibers = new LongObjMap<>(32, 0.6f);

    @SuppressWarnings("FieldMayBeFinal")
    private volatile boolean shouldStop = false;
    private final static VarHandle SHOULD_STOP;

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

    public FiberGroup(String name, Dispatcher dispatcher) {
        this.name = name;
        this.dispatcher = dispatcher;
        this.shouldStopCondition = newCondition(name + "-shouldStop");
    }

    /**
     * can call in any thread
     */
    public void fireFiber(Fiber fiber) {
        if (fiber.fiberGroup != this) {
            throw new DtException("fiber not in group");
        }
        boolean b = dispatcher.doInDispatcherThread(new FiberQueueTask() {
            @Override
            protected void run() {
                start(fiber, false);
            }
        });
        if (!b) {
            log.info("dispatcher is shutdown, ignore fireFiber");
        }
    }

    /**
     * can call in any thread
     */
    public void fireFiber(String fiberName, FiberFrame<Void> firstFrame) {
        fireFiber(new Fiber(fiberName, this, firstFrame));
    }

    /**
     * can call in any thread
     */
    public void requestShutdown() {
        // if the dispatcher stopped, no ops
        Fiber shutdownGroupFiber = new Fiber("shutdownGroup", this, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if ((boolean) SHOULD_STOP.get(FiberGroup.this)) {
                    return Fiber.frameReturn();
                }
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

    public <T> FiberFuture<T> newFuture() {
        return new FiberFuture<>(this);
    }

    public <T> FiberChannel<T> newChannel() {
        return new FiberChannel<>(this);
    }

    public FiberLock newLock() {
        return new FiberLock(this);
    }

    void checkGroup() {
        if (DispatcherThread.currentGroup() != this) {
            throw new FiberException("not in current group: " + name);
        }
    }

    void start(Fiber f, boolean addFirst) {
        if (f.fiberGroup.finished) {
            log.warn("group finished, ignore fiber start: {}", f.getFiberName());
            return;
        }
        if (f.started) {
            BugLog.getLog().error("fiber already started: {}", f.getFiberName());
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
        tryMakeFiberReady(f, false);
    }

    void removeFiber(Fiber f) {
        boolean removed;
        if (f.daemon) {
            removed = daemonFibers.remove(f.id) != null;
        } else {
            removed = normalFibers.remove(f.id) != null;
        }
        if (!removed) {
            BugLog.getLog().error("fiber is not in set: {}", f.getFiberName());
        }
    }

    void tryMakeFiberReady(Fiber f, boolean addFirst) {
        if (finished) {
            log.warn("group finished, ignore makeReady: {}", f.getFiberName());
            return;
        }
        if (f.finished) {
            log.warn("fiber already finished, ignore makeReady: {}", f.getFiberName());
            return;
        }
        if (!f.ready) {
            f.ready = true;
            if (f.signalInThisRound) {
                if (addFirst) {
                    readyFibersNextRound.addFirst(f);
                } else {
                    readyFibersNextRound.addLast(f);
                }
            } else {
                if (addFirst) {
                    readyFibers.addFirst(f);
                } else {
                    readyFibers.addLast(f);
                }
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
        boolean ss = (boolean) SHOULD_STOP.get(this);
        if (ss && !finished) {
            finished = normalFibers.size() == 0 && readyFibersNextRound.size() == 0;
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
            boolean b = dispatcher.doInDispatcherThread(new FiberQueueTask() {
                @Override
                protected void run() {
                    logGroupInfo0(msg);
                    f.complete(null);
                }
            });
            if (!b) {
                log.error("dispatcher is shutdown and can't log group info");
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
                .append(", readyNext=").append(readyFibersNextRound.size())
                .append(", normal=").append(normalFibers.size())
                .append(", daemon=").append(daemonFibers.size())
                .append("\n")
                .append("--------------------------------------------------\n")
                .append("readyFibers:\n");
        concatReadyFibers(readyFibers, sb);
        concatReadyFibers(readyFibersNextRound, sb);
        sb.append("--------------------------------------------------\n");
        sb.append("normalFibers:\n");
        normalFibers.forEach((key, f) -> {
            concatFiberName(sb, f);
            sb.append(", waitOn=").append(f.source).append('\n');
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
        sb.append(f.getFiberName()).append("(").append(Integer.toHexString(f.hashCode())).append(")");
        if (f.lastWaitFor != null) {
            sb.append(", lastWaitFor=").append(f.lastWaitFor);
        }
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public CompletableFuture<Void> getShutdownFuture() {
        return shutdownFuture;
    }
}
