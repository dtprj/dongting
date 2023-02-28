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
package com.github.dtprj.dongting.raft.impl;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class RaftExecutor implements Executor {
    private final LinkedBlockingQueue<Object> queue;
    private final static ScheduledExecutorService SCHEDULED_SERVICE = Executors.newSingleThreadScheduledExecutor();

    public RaftExecutor(LinkedBlockingQueue<Object> queue) {
        this.queue = queue;
    }

    public void schedule(Runnable runnable, long delayMillis) {
        if (delayMillis <= 0) {
            execute(runnable);
        } else {
            // ScheduledExecutorService just for schedule, the runnable will be executed in the RaftThread
            RaftExecutor executor = this;
            Runnable wrapper = () ->  executor.execute(runnable);
            SCHEDULED_SERVICE.schedule(wrapper, delayMillis, TimeUnit.MILLISECONDS);
        }
    }


    @Override
    public void execute(Runnable command) {
        queue.offer(command);
    }

    public LinkedBlockingQueue<Object> getQueue() {
        return queue;
    }
}
