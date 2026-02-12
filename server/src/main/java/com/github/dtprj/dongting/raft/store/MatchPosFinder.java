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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftCancelException;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class MatchPosFinder extends FiberFrame<Pair<Integer, Long>> {
    private final Supplier<Boolean> cancel;
    private final int suggestTerm;
    private final long suggestIndex;
    private final RaftGroupConfigEx groupConfig;
    private final IndexedQueue<LogFile> queue;
    private final IdxOps idxOps;
    private final TailCache tailCache;
    private final long fileLenMask;

    private LogFile logFile;

    private long leftIndex;
    private int leftTerm;
    private long rightIndex;
    private long midIndex;
    private ByteBuffer buf;//no need release

    MatchPosFinder(RaftGroupConfigEx groupConfig, IndexedQueue<LogFile> queue, IdxOps idxOps, Supplier<Boolean> cancel,
                   TailCache tailCache, long fileLenMask, int suggestTerm, long suggestIndex, long lastLogIndex) {
        this.groupConfig = groupConfig;
        this.queue = queue;
        this.idxOps = idxOps;
        this.cancel = cancel;
        this.tailCache = tailCache;
        this.fileLenMask = fileLenMask;
        this.suggestTerm = suggestTerm;
        this.suggestIndex = suggestIndex;

        this.rightIndex = Math.min(suggestIndex, lastLogIndex);
    }

    private void checkCancel() {
        if (cancel.get()) {
            throw new RaftCancelException("find match pos cancelled");
        }
    }

    @Override
    public FrameCallResult execute(Void input) throws Throwable {
        Pair<Integer, Long> r = findInCache();
        if (r != null) {
            setResult(r);
            return Fiber.frameReturn();
        }
        logFile = findMatchLogFile();
        if (logFile == null) {
            setResult(null);
            return Fiber.frameReturn();
        } else {
            if (tailCache.getFirstIndex() > 0) {
                rightIndex = Math.min(tailCache.getFirstIndex(), rightIndex);
            }
            this.leftIndex = logFile.firstIndex;
            this.leftTerm = logFile.firstTerm;
            buf = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE);
            return loop(null);
        }
    }

    private Pair<Integer, Long> findInCache() {
        long li = tailCache.getFirstIndex();
        RaftTask task = tailCache.get(li);
        if (task == null) {
            return null;
        }
        int lt = task.item.term;
        if (!valid(lt, li)) {
            return null;
        }
        long ri = rightIndex;
        while (li < ri) {
            long mi = computeMidIndex(li, ri);
            task = tailCache.get(mi);
            if (task == null) {
                BugLog.log("middle index not in tail cache: {}", mi);
                break;
            }
            int mt = task.item.term;
            if (valid(mt, mi)) {
                li = mi;
                lt = mt;
            } else {
                ri = mi - 1;
            }
        }
        return new Pair<>(lt, li);
    }

    private boolean valid(int term, long index) {
        return (term <= suggestTerm && index < suggestIndex) || (term == suggestTerm && index == suggestIndex);
    }


    private LogFile findMatchLogFile() {
        if (queue.size() == 0) {
            return null;
        }
        int left = 0;
        int right = queue.size() - 1;
        while (left <= right) {
            int mid = (left + right + 1) >>> 1;
            LogFile logFile = queue.get(mid);
            if (logFile.shouldDelete()) {
                left = mid + 1;
                continue;
            }
            if (logFile.firstIndex == 0) {
                right = mid - 1;
                continue;
            }
            if (valid(logFile.firstTerm, logFile.firstIndex)) {
                if (left == right) {
                    return logFile;
                } else {
                    left = mid;
                }
            } else {
                right = mid - 1;
            }
        }
        return null;
    }

    private FrameCallResult loop(Void input) {
        checkCancel();
        if (leftIndex < rightIndex) {
            midIndex = computeMidIndex(leftIndex, rightIndex);
            return Fiber.call(idxOps.loadLogPos(midIndex), this::posLoadComplete);
        } else {
            setResult(new Pair<>(leftTerm, leftIndex));
            return Fiber.frameReturn();
        }
    }

    private static long computeMidIndex(long li, long ri) {
        return (li + ri + 1) >>> 1;
    }

    private FrameCallResult posLoadComplete(Long pos) {
        checkCancel();
        if (pos >= logFile.endPos) {
            // the right index may not in the current logFile, because:
            // 1, the first index of tail cache is not in the current logFile
            // 2, the suggest index is not in the current logFile, and tail cache is empty
            rightIndex = midIndex - 1;
            return Fiber.resume(null, this::loop);
        }

        AsyncIoTask task = new AsyncIoTask(groupConfig.fiberGroup, logFile);
        buf.clear();
        FiberFuture<Void> f = task.read(buf, pos & fileLenMask);
        return f.await(this::headerLoadComplete);
    }

    private FrameCallResult headerLoadComplete(Void v) {
        checkCancel();

        buf.flip();
        LogHeader header = new LogHeader();
        header.read(buf);
        if (!header.crcMatch()) {
            throw new ChecksumException();
        }
        if (header.index != midIndex) {
            throw new RaftException("index not match");
        }
        if (valid(header.term, midIndex)) {
            leftIndex = midIndex;
            leftTerm = header.term;
        } else {
            rightIndex = midIndex - 1;
        }
        return Fiber.resume(null, this::loop);
    }

}
