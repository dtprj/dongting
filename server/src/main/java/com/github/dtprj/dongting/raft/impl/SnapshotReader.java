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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.sm.Snapshot;

import java.util.LinkedList;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class SnapshotReader extends FiberFrame<Void> {

    private static final DtLog log = DtLogs.getLogger(SnapshotReader.class);

    private final Snapshot snapshot;
    private final int maxReadConcurrency;
    private final int maxWriteConcurrency;
    private final BiFunction<RefBuffer, Integer, FiberFuture<Void>> callback;
    private final Supplier<Boolean> cancel;
    private final Supplier<RefBuffer> bufferCreator;

    private final FiberCondition cond;

    private final LinkedList<Pair<RefBuffer, FiberFuture<Integer>>> readList = new LinkedList<>();
    private final LinkedList<FiberFuture<Void>> writeList = new LinkedList<>();

    private Throwable firstEx;
    private boolean readFinish;

    // the callback should release the buffer
    public SnapshotReader(Snapshot snapshot, int maxReadConcurrency, int maxWriteConcurrency,
                          BiFunction<RefBuffer, Integer, FiberFuture<Void>> callback,
                          Supplier<Boolean> cancel,
                          Supplier<RefBuffer> bufferCreator) {
        this.snapshot = snapshot;
        this.maxReadConcurrency = maxReadConcurrency;
        this.maxWriteConcurrency = maxWriteConcurrency;
        this.callback = callback;
        this.cancel = cancel;
        this.bufferCreator = bufferCreator;
        this.cond = FiberGroup.currentGroup().newCondition("snapshotReaderCond");
    }

    @Override
    public FrameCallResult execute(Void input) throws Throwable {
        int oldSize = readList.size();
        processReadResult();
        boolean readListProcessed = oldSize != readList.size();
        oldSize = writeList.size();
        processWriteResult();
        boolean writeListProcessed = oldSize != writeList.size();
        boolean addNewReadTask = false;
        if (!readFinish && firstEx == null && !cancel.get()) {
            if (readList.size() < maxReadConcurrency) {
                // fire read task
                RefBuffer buf = bufferCreator.get();
                addNewReadTask = true;
                FiberFuture<Integer> future = snapshot.readNext(buf.getBuffer());
                readList.add(new Pair<>(buf, future));
                future.registerCallback((v, ex) -> cond.signal());
            }
        } else {
            if (readList.isEmpty() && writeList.isEmpty()) {
                if (firstEx != null) {
                    throw firstEx;
                } else {
                    if (readFinish) {
                        log.info("snapshot read finished: {}", snapshot.getClass().getSimpleName());
                        return Fiber.frameReturn();
                    } else {
                        log.info("snapshot read canceled: {}", snapshot.getClass().getSimpleName());
                        throw new RaftCancelException("snapshot read canceled");
                    }
                }
            }
        }
        if (readListProcessed || writeListProcessed || addNewReadTask) {
            return Fiber.yield(this);
        } else {
            return cond.await(1000, this);
        }
    }

    private void processReadResult() {
        Pair<RefBuffer, FiberFuture<Integer>> pair = readList.peekFirst();
        if (pair == null) {
            return;
        }
        FiberFuture<Integer> f = pair.getRight();
        RefBuffer buf = pair.getLeft();
        if (!f.isDone()) {
            // header in list not finished, wait for next time
            return;
        }
        if (f.getEx() != null && firstEx == null) {
            // only record the first exception
            firstEx = f.getEx();
        }
        if (f.getResult() != null && f.getResult() == 0) {
            readFinish = true;
        }
        if (cancel.get() || firstEx != null || readFinish) {
            readList.removeFirst();
            buf.release();
        } else if (writeList.size() < maxWriteConcurrency) {
            readList.removeFirst();
            try {
                FiberFuture<Void> writeFuture = callback.apply(buf, f.getResult());
                writeList.add(writeFuture);
                writeFuture.registerCallback((v, ex) -> cond.signal());
            } catch (Throwable e) {
                BugLog.log(e);
                firstEx = e;
            }
        } else {
            // should wait for write, so not remove from readList
        }
    }

    private void processWriteResult() {
        FiberFuture<Void> f;
        while ((f = writeList.peekFirst()) != null) {
            if (!f.isDone()) {
                // header in list not finished, wait for next time
                break;
            }
            if (f.getEx() != null && firstEx == null) {
                // only record the first exception
                firstEx = f.getEx();
            }
            writeList.removeFirst();
        }
    }

}
