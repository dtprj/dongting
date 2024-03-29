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
package com.github.dtprj.dongting.common;

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public abstract class AbstractLifeCircle implements LifeCircle {
    private static final DtLog log = DtLogs.getLogger(AbstractLifeCircle.class);

    public static final int STATUS_NOT_START = 0;
    public static final int STATUS_STARTING = 100;
    public static final int STATUS_RUNNING = 200;
    public static final int STATUS_PREPARE_STOP = 300;
    public static final int STATUS_STOPPING = 400;
    public static final int STATUS_STOPPED = 500;

    protected volatile int status = STATUS_NOT_START;
    private final ReentrantLock lock = new ReentrantLock();
    protected final CompletableFuture<Void> prepareStopFuture = new CompletableFuture<>();

    public int getStatus() {
        return status;
    }

    @Override
    public final void start() {
        lock.lock();
        try {
            if (status == STATUS_NOT_START) {
                status = STATUS_STARTING;
                doStart();
                status = STATUS_RUNNING;
            } else {
                throw new IllegalStateException("error state: " + status);
            }
        } finally {
            lock.unlock();
        }
    }

    protected abstract void doStart();

    @Override
    public final void stop(DtTime timeout) {
        Objects.requireNonNull(timeout);
        lock.lock();
        try {
            switch (status) {
                case STATUS_NOT_START:
                    log.error("status is not_start, skip stop");
                    status = STATUS_STOPPED;
                    return;
                case STATUS_STARTING:
                    log.error("status is starting, try force stop");
                    status = STATUS_STOPPING;
                    doStop(timeout, true);
                    status = STATUS_STOPPED;
                    return;
                case STATUS_RUNNING:
                case STATUS_PREPARE_STOP:
                    status = STATUS_STOPPING;
                    doStop(timeout, false);
                    status = STATUS_STOPPED;
                    return;
                case STATUS_STOPPING:
                    log.error("last stop failed, skip stop");
                    return;
                case STATUS_STOPPED:
                    // no op
                    return;
                default:
                    throw new IllegalStateException("error state: " + status);
            }
        } finally {
            lock.unlock();
        }
    }

    protected abstract void doStop(DtTime timeout, boolean force);

    protected CompletableFuture<Void> prepareStop() {
        lock.lock();
        try {
            switch (status) {
                case STATUS_NOT_START:
                    log.error("status is not_start");
                    return CompletableFuture.completedFuture(null);
                case STATUS_STARTING:
                    log.error("status is starting");
                case STATUS_RUNNING:
                case STATUS_PREPARE_STOP:
                    this.status = STATUS_PREPARE_STOP;
                    return prepareStopFuture;
                case STATUS_STOPPING:
                    log.error("status is stopping");
                    return CompletableFuture.completedFuture(null);
                case STATUS_STOPPED:
                    log.error("status is stopped");
                    return CompletableFuture.completedFuture(null);
                default:
                    throw new IllegalStateException("error state: " + status);
            }
        } finally {
            lock.unlock();
        }
    }

}
