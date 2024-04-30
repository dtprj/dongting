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
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.RaftException;
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
    private final long lastLogIndex;
    private final RaftGroupConfigEx groupConfig;
    private final IndexedQueue<LogFile> queue;
    private final IdxOps idxOps;
    private final long fileLenMask;

    private LogFile logFile;

    private long leftIndex;
    private int leftTerm;
    private long rightIndex;
    private long midIndex;
    private ByteBuffer buf;//no need release

    MatchPosFinder(RaftGroupConfigEx groupConfig, IndexedQueue<LogFile> queue, IdxOps idxOps, Supplier<Boolean> cancel, long fileLenMask,
                   int suggestTerm, long suggestIndex, long lastLogIndex) {
        this.groupConfig = groupConfig;
        this.queue = queue;
        this.idxOps = idxOps;
        this.cancel = cancel;
        this.fileLenMask = fileLenMask;
        this.suggestTerm = suggestTerm;
        this.suggestIndex = suggestIndex;
        this.lastLogIndex = lastLogIndex;
    }

    private void checkCancel() {
        if (cancel.get()) {
            throw new RaftException("find match pos cancelled");
        }
    }

    @Override
    public FrameCallResult execute(Void input) throws Throwable {
        logFile = findMatchLogFile(suggestTerm, suggestIndex);
        if (logFile == null) {
            setResult(null);
            return Fiber.frameReturn();
        } else {
            this.leftIndex = logFile.firstIndex;
            this.leftTerm = logFile.firstTerm;
            this.rightIndex = Math.min(suggestIndex, lastLogIndex);
            buf = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE);
            return Fiber.resume(null, this::loop);
        }
    }

    private LogFile findMatchLogFile(int suggestTerm, long suggestIndex) {
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
            if (logFile.firstIndex > suggestIndex) {
                right = mid - 1;
                continue;
            }
            if (logFile.firstIndex == suggestIndex && logFile.firstTerm == suggestTerm) {
                return logFile;
            } else if (logFile.firstIndex < suggestIndex && logFile.firstTerm <= suggestTerm) {
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
            midIndex = (leftIndex + rightIndex + 1) >>> 1;
            return idxOps.loadLogPos(midIndex, this::posLoadComplete);
        } else {
            setResult(new Pair<>(leftTerm, leftIndex));
            return Fiber.frameReturn();
        }
    }

    private FrameCallResult posLoadComplete(Long pos) {
        checkCancel();
        if (pos >= logFile.endPos) {
            BugLog.getLog().error("pos >= logFile.endPos, pos={}, logFile={}", pos, logFile);
            throw new RaftException(new RaftException("pos >= logFile.endPos"));
        }

        AsyncIoTask task = new AsyncIoTask(groupConfig, logFile);
        buf.clear();
        FiberFrame<Void> f = task.lockRead(buf, pos & fileLenMask);
        return Fiber.call(f, this::headerLoadComplete);
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
        if (midIndex == suggestIndex && header.term == suggestTerm) {
            setResult(new Pair<>(header.term, midIndex));
            return Fiber.frameReturn();
        } else if (midIndex < suggestIndex && header.term <= suggestTerm) {
            leftIndex = midIndex;
            leftTerm = header.term;
        } else {
            rightIndex = midIndex - 1;
        }
        return Fiber.resume(null, this::loop);
    }

}
