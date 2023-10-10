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
    private final LinkedBlockingQueue<Object> shareQueue;
    private final RaftStatusImpl raftStatus;
    private final Timestamp ts;
    private final Raft raft;

    private final ArrayList<RaftTask> leaderTasks = new ArrayList<>(32);
    private final ArrayList<Runnable> runnables = new ArrayList<>(32);
    private final ArrayList<Object> localQueue = new ArrayList<>(32);

    private boolean poll = true;

    public Dispatcher(LinkedBlockingQueue<Object> shareQueue, RaftStatusImpl raftStatus, Raft raft) {
        this.shareQueue = shareQueue;
        this.raftStatus = raftStatus;
        this.ts = raftStatus.getTs();
        this.raft = raft;
    }

    public void runOnce() throws InterruptedException {
        Timestamp ts = this.ts;
        ArrayList<Object> localQueue = this.localQueue;
        pollAndRefreshTs(ts, localQueue);
        process(leaderTasks, runnables, localQueue);
        if (!localQueue.isEmpty()) {
            ts.refresh(1);
            localQueue.clear();
        }
    }

    private void process(ArrayList<RaftTask> rwTasks, ArrayList<Runnable> runnables, ArrayList<Object> localQueue) {
        RaftStatusImpl raftStatus = this.raftStatus;
        int len = localQueue.size();
        for (int i = 0; i < len; i++) {
            Object o = localQueue.get(i);
            if (o instanceof RaftTask) {
                rwTasks.add((RaftTask) o);
            } else {
                runnables.add((Runnable) o);
            }
        }

        // the sequence of RaftTask and Runnable is reordered, but it will not affect the linearizability
        len = runnables.size();
        boolean exec = false;
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                runnables.get(i).run();
                exec = true;
            }
            runnables.clear();
        }

        if (!rwTasks.isEmpty()) {
            if (!raftStatus.isHoldRequest()) {
                raft.raftExec(rwTasks);
                rwTasks.clear();
                exec = true;
            }
        }

        while (raftStatus.getLastPersistLogIndex() == raftStatus.getLastLogIndex()
                && raftStatus.getWaitWriteFinishedQueue().size() > 0) {
            raftStatus.getWaitWriteFinishedQueue().removeFirst().run();
            exec = true;
        }

        while(!raftStatus.isWaitAppend() && raftStatus.getWaitAppendQueue().size() > 0){
            raftStatus.getWaitAppendQueue().removeFirst().run();
            exec = true;
        }

        if (exec) {
            raftStatus.copyShareStatus();
        }
    }

    private void pollAndRefreshTs(Timestamp ts, ArrayList<Object> queueData) throws InterruptedException {
        long oldNanos = ts.getNanoTime();
        if (poll) {
            Object o = shareQueue.poll(50, TimeUnit.MILLISECONDS);
            if (o != null) {
                queueData.add(o);
            }
        } else {
            shareQueue.drainTo(queueData);
        }

        ts.refresh(1);
        poll = ts.getNanoTime() - oldNanos > 2 * 1000 * 1000 || queueData.isEmpty();
    }


}
