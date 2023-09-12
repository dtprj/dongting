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

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public abstract class AbstractLifeCircle implements LifeCircle {
    private static final DtLog log = DtLogs.getLogger(AbstractLifeCircle.class);

    public enum LifeStatus {
        not_start, starting, running, stopping, stopped
    }

    protected volatile LifeStatus status = LifeStatus.not_start;
    private final ReentrantLock lock = new ReentrantLock();

    public LifeStatus getStatus() {
        return status;
    }

    @Override
    public final void start() {
        lock.lock();
        try {
            if (status == LifeStatus.not_start) {
                status = LifeStatus.starting;
                doStart();
                status = LifeStatus.running;
            } else {
                throw new IllegalStateException("error state: " + status);
            }
        } finally {
            lock.unlock();
        }
    }

    protected abstract void doStart();

    @Override
    public final void stop() {
        lock.lock();
        try {
            switch (status) {
                case stopped:
                    return;
                case starting:
                    log.warn("status is starting, try stop");
                    status = LifeStatus.stopping;
                    doStop(true);
                    status = LifeStatus.stopped;
                    return;
                case running:
                    status = LifeStatus.stopping;
                    doStop(false);
                    status = LifeStatus.stopped;
                    return;
                case not_start:
                    log.warn("status is not_start, skip stop");
                    status = LifeStatus.stopped;
                    return;
                case stopping:
                    log.warn("last stop failed, skip stop");
            }
        } finally {
            lock.unlock();
        }
    }

    protected abstract void doStop(boolean force);

}
