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

import com.github.dtprj.dongting.common.Timestamp;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class Dispatcher {
    private final LinkedBlockingQueue<Object> queue;
    private final RaftStatusImpl raftStatus;
    private final Timestamp ts;
    private final Raft raft;

    private final ArrayList<RaftTask> leaderTasks = new ArrayList<>(32);
    private final ArrayList<Runnable> runnables = new ArrayList<>(32);
    private final ArrayList<Object> queueData = new ArrayList<>(32);

    private boolean poll = true;

    public Dispatcher(LinkedBlockingQueue<Object> queue, RaftStatusImpl raftStatus, Raft raft) {
        this.queue = queue;
        this.raftStatus = raftStatus;
        this.ts = raftStatus.getTs();
        this.raft = raft;
    }

    public void runOnce() throws InterruptedException {
        Timestamp ts = this.ts;
        pollAndRefreshTs(ts, queueData);
        process(leaderTasks, runnables, queueData);
        if (!queueData.isEmpty()) {
            ts.refresh(1);
            queueData.clear();
        }
    }

    private void process(ArrayList<RaftTask> rwTasks, ArrayList<Runnable> runnables, ArrayList<Object> queueData) {
        RaftStatusImpl raftStatus = this.raftStatus;
        int len = queueData.size();
        for (int i = 0; i < len; i++) {
            Object o = queueData.get(i);
            if (o instanceof RaftTask) {
                rwTasks.add((RaftTask) o);
            } else {
                runnables.add((Runnable) o);
            }
        }

        // the sequence of RaftTask and Runnable is reordered, but it will not affect the linearizability
        if (!rwTasks.isEmpty()) {
            if (!raftStatus.isHoldRequest()) {
                raft.raftExec(rwTasks);
                rwTasks.clear();
                raftStatus.copyShareStatus();
            }
        }
        len = runnables.size();
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                runnables.get(i).run();
            }
            runnables.clear();
            raftStatus.copyShareStatus();
        }
    }

    private void pollAndRefreshTs(Timestamp ts, ArrayList<Object> queueData) throws InterruptedException {
        long oldNanos = ts.getNanoTime();
        if (poll) {
            Object o = queue.poll(50, TimeUnit.MILLISECONDS);
            if (o != null) {
                queueData.add(o);
            }
        } else {
            queue.drainTo(queueData);
        }

        ts.refresh(1);
        poll = ts.getNanoTime() - oldNanos > 2 * 1000 * 1000 || queueData.isEmpty();
    }


}
