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

import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.ChecksumException;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class MatchPosFinder {
    private final Supplier<Boolean> stopIndicator;
    private final Supplier<Boolean> cancel;
    private final LogFile logFile;
    private final int suggestTerm;
    private final long suggestRightBoundIndex;
    private final Executor raftExecutor;
    private final IdxOps idxOps;
    private final long fileLenMask;


    private final CompletableFuture<Pair<Integer, Long>> future = new CompletableFuture<>();

    private long leftIndex;
    private int leftTerm;
    private long rightIndex;
    private long midIndex;

    MatchPosFinder(IdxOps idxOps, Executor raftExecutor, Supplier<Boolean> stopIndicator,
                   Supplier<Boolean> cancel, long fileLenMask,
                   LogFile logFile, int suggestTerm, long suggestRightBoundIndex) {
        this.idxOps = idxOps;
        this.raftExecutor = raftExecutor;
        this.stopIndicator = stopIndicator;
        this.cancel = cancel;
        this.fileLenMask = fileLenMask;
        this.logFile = logFile;
        this.suggestTerm = suggestTerm;
        this.suggestRightBoundIndex = suggestRightBoundIndex;

        this.leftIndex = logFile.firstIndex;
        this.leftTerm = logFile.firstTerm;
        this.rightIndex = suggestRightBoundIndex;
    }

    void exec() {
        try {
            if (cancel.get()) {
                future.cancel(false);
                return;
            }
            logFile.use++;
            if (leftIndex < rightIndex) {
                midIndex = (leftIndex + rightIndex + 1) >>> 1;
                CompletableFuture<Long> posFuture = idxOps.loadLogPos(midIndex);
                posFuture.whenCompleteAsync((r, ex) -> posLoadComplete(ex, r), raftExecutor);
            } else {
                complete(new Pair<>(leftTerm, leftIndex), null);
            }
        } catch (Throwable e) {
            complete(null, e);
        }
    }

    private void complete(Pair<Integer, Long> result, Throwable ex) {
        logFile.use--;
        if (ex != null) {
            future.completeExceptionally(ex);
        } else {
            future.complete(result);
        }
    }

    private boolean failOrCancel(Throwable ex) {
        if (ex != null) {
            complete(null, ex);
            return true;
        }
        if (cancel.get()) {
            logFile.use--;
            future.cancel(false);
            return true;
        }
        return false;
    }

    private void posLoadComplete(Throwable ex, Long pos) {
        try {
            if (failOrCancel(ex)) {
                return;
            }
            if (pos >= logFile.endPos) {
                BugLog.getLog().error("pos >= logFile.endPos, pos={}, logFile={}", pos, logFile);
                complete(null, new RaftException("pos >= logFile.endPos"));
                return;
            }

            AsyncIoTask task = new AsyncIoTask(logFile.channel, stopIndicator, cancel);
            ByteBuffer buf = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE);
            CompletableFuture<Void> f = task.read(buf, pos & fileLenMask);
            f.whenCompleteAsync((v, loadHeaderEx) -> headerLoadComplete(loadHeaderEx, buf), raftExecutor);
        } catch (Throwable e) {
            complete(null, e);
        }
    }

    private void headerLoadComplete(Throwable ex, ByteBuffer buf) {
        try {
            if (failOrCancel(ex)) {
                return;
            }

            buf.flip();
            LogHeader header = new LogHeader();
            header.read(buf);
            if (!header.crcMatch()) {
                complete(null, new ChecksumException());
                return;
            }
            if (header.index != midIndex) {
                complete(null, new RaftException("index not match"));
                return;
            }
            if (midIndex == suggestRightBoundIndex && header.term == suggestTerm) {
                complete(new Pair<>(header.term, midIndex), null);
                return;
            } else if (midIndex < suggestRightBoundIndex && header.term <= suggestTerm) {
                leftIndex = midIndex;
                leftTerm = header.term;
            } else {
                rightIndex = midIndex - 1;
            }
            exec();
        } catch (Throwable e) {
            complete(null, e);
        }
    }

    public CompletableFuture<Pair<Integer, Long>> getFuture() {
        return future;
    }
}
