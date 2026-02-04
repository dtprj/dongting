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
package com.github.dtprj.dongting.perf;

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public abstract class SimplePerf extends PerfCallback {
    protected static final DtLog log = DtLogs.getLogger(SimplePerf.class);
    private final boolean useCollectExecutor;

    private ScheduledExecutorService ses = DtUtil.SCHEDULED_SERVICE;
    private ScheduledFuture<?> future;
    private boolean started;
    private volatile boolean shutdown;

    public SimplePerf(boolean useNanos, boolean useCollectExecutor) {
        super(useNanos);
        this.useCollectExecutor = useCollectExecutor;
    }

    @SuppressWarnings("unused")
    public void setScheduler(ScheduledExecutorService ses) {
        this.ses = ses;
    }

    @Override
    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        scheduleNext();
    }

    @Override
    public synchronized void shutdown() {
        shutdown = true;
        if (future != null) {
            future.cancel(false);
        }
    }

    private void scheduleNext() {
        if (shutdown) {
            return;
        }
        try {
            long now = System.currentTimeMillis();
            long delay = 60_000 - (now % 60_000);
            future = ses.schedule(this::run, delay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            log.error("schedule rejected");
        } catch (Exception e) {
            log.error("schedule error", e);
        }
    }

    private void run() {
        if (shutdown) {
            return;
        }
        if (!useCollectExecutor || collectExecutor == null) {
            run0();
        } else {
            try {
                collectExecutor.execute(this::run0);
            } catch (RejectedExecutionException e) {
                log.error("executor rejected");
            }
        }
    }

    private void run0() {
        if (shutdown) {
            return;
        }
        try {
            collect();
        } catch (Exception e) {
            log.error("reset and log error", e);
        } finally {
            scheduleNext();
        }
    }

    protected abstract void collect();
}
