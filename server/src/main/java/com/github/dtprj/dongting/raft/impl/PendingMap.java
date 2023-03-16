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

import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.log.BugLog;

import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class PendingMap extends LongObjMap<RaftTask> {
    private static final long TIMEOUT = TimeUnit.SECONDS.toNanos(10);
    private long firstKey = -1;
    private int pending;
    private long pendingBytes;

    @Override
    public RaftTask put(long key, RaftTask value) {
        RaftTask t = super.put(key, value);
        if (size() == 1) {
            firstKey = key;
        } else {
            if (key <= firstKey) {
                BugLog.getLog().error("key {} is not great than firstKey {}", key, firstKey);
            }
        }
        pending++;
        pendingBytes += value.input.size();
        return t;
    }

    @Override
    public RaftTask remove(long key) {
        if (key > firstKey && firstKey != -1) {
            BugLog.getLog().error("key {} is greater than firstKey {}", key, firstKey);
        }
        RaftTask t = super.remove(key);
        if (t != null) {
            pending--;
            pendingBytes -= t.input.size();
        }
        if (size() == 0) {
            firstKey = -1;
        }
        return t;
    }

    @Override
    public void forEach(Visitor<RaftTask> visitor) {
        Visitor<RaftTask> newVisitor = (k, v) -> {
            if (!visitor.visit(k, v)) {
                throw new UnsupportedOperationException();
            }
            return true;
        };
        super.forEach(newVisitor);
    }

    public void cleanPending(RaftStatus raftStatus, int maxPending, long maxPendingBytes) {
        if (firstKey <= 0) {
            return;
        }
        if (raftStatus.getRole() == RaftRole.leader) {
            long minMatchIndex = Long.MAX_VALUE;
            for (RaftMember node : raftStatus.getMembers()) {
                minMatchIndex = Math.min(node.getMatchIndex(), minMatchIndex);
            }
            doClean(raftStatus, maxPending, maxPendingBytes, minMatchIndex);
        } else {
            doClean(raftStatus, maxPending, maxPendingBytes, raftStatus.getLastApplied());
        }
    }

    private void doClean(RaftStatus raftStatus, int maxPending, long maxPendingBytes, long boundIndex) {
        long now = raftStatus.getTs().getNanoTime();
        long k = firstKey;
        RaftTask task = get(k);
        while (task != null) {
            if (k > boundIndex && now - task.createTimeNanos < TIMEOUT) {
                if (pending <= maxPending && pendingBytes <= maxPendingBytes) {
                    break;
                }
            }
            remove(k);
            task = get(++k);
        }
    }
}

