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
public class PendingMap {
    private static final long TIMEOUT = TimeUnit.SECONDS.toNanos(10);
    private long firstKey = -1;
    private int pending;
    private long pendingBytes;
    private final LongObjMap<RaftTask> map = new LongObjMap<>();

    public RaftTask get(long key) {
        return map.get(key);
    }

    public RaftTask put(long key, RaftTask value) {
        RaftTask t = map.put(key, value);
        if (map.size() == 1) {
            firstKey = key;
        } else {
            if (key <= firstKey) {
                BugLog.getLog().error("key {} is not great than firstKey {}", key, firstKey);
            }
        }
        pending++;
        pendingBytes += value.getInput().getFlowControlSize();
        return t;
    }

    public RaftTask remove(long key) {
        if (key > firstKey && firstKey != -1) {
            BugLog.getLog().error("key {} is greater than firstKey {}", key, firstKey);
        }
        RaftTask t = map.remove(key);
        if (t != null) {
            pending--;
            pendingBytes -= t.getInput().getFlowControlSize();
            RaftTask x = t;
            while (x != null) {
                x.getItem().release();
                x = x.getNextReader();
            }
        }
        if (map.size() == 0) {
            firstKey = -1;
        }
        return t;
    }

    public void forEach(LongObjMap.ReadOnlyVisitor<RaftTask> visitor) {
        map.forEach(visitor);
    }

    public void cleanPending(RaftStatusImpl raftStatus, int maxPending, long maxPendingBytes) {
        if (firstKey <= 0) {
            return;
        }
        if (raftStatus.getRole() == RaftRole.leader) {
            long minMatchIndex = Long.MAX_VALUE;
            for (RaftMember node : raftStatus.getMembers()) {
                minMatchIndex = Math.min(node.getMatchIndex(), minMatchIndex);
            }
            for (RaftMember node : raftStatus.getPreparedMembers()) {
                minMatchIndex = Math.min(node.getMatchIndex(), minMatchIndex);
            }
            doClean(raftStatus, maxPending, maxPendingBytes, minMatchIndex);
        } else {
            doClean(raftStatus, maxPending, maxPendingBytes, raftStatus.getLastApplied());
        }
    }

    private void doClean(RaftStatusImpl raftStatus, int maxPending, long maxPendingBytes, long boundIndex) {
        long now = raftStatus.getTs().getNanoTime();
        long k = firstKey;
        RaftTask task = map.get(k);
        while (task != null) {
            if (k > boundIndex && now - task.getCreateTimeNanos() < TIMEOUT) {
                if (pending <= maxPending && pendingBytes <= maxPendingBytes) {
                    break;
                }
            }
            remove(k);
            task = map.get(++k);
        }
    }
}

