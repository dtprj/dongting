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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCancelException;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberInterruptException;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.HandlerFrame;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.function.Supplier;

/**
 * @author huangli
 */
public class RetryFrame<O> extends FiberFrame<O> {

    private static final DtLog log = DtLogs.getLogger(RetryFrame.class);

    private final FiberFrame<O> subFrame;
    private final int[] retryIntervals;
    private final boolean retryForever;
    private final Supplier<Boolean> cancelRetry;
    private int retryCount;
    private Throwable lastSubFrameEx;

    public RetryFrame(FiberFrame<O> subFrame, int[] retryIntervals, boolean retryForever, Supplier<Boolean> cancelRetry) {
        this.subFrame = subFrame;
        this.retryIntervals = retryIntervals;
        this.retryForever = retryForever;
        this.cancelRetry = cancelRetry;
    }

    @Override
    public FrameCallResult execute(Void input) {
        return Fiber.call(new HandlerFrame<>(subFrame), this::resume);
    }

    private FrameCallResult resume(Pair<O, Throwable> result) throws Throwable {
        Throwable subFrameEx = result.getRight();
        if (subFrameEx == null) {
            setResult(result.getLeft());
            return Fiber.frameReturn();
        } else {
            Throwable root = DtUtil.rootCause(subFrameEx);
            if (root instanceof FiberInterruptException || root instanceof FiberCancelException
                    || root instanceof InterruptedException) {
                throw subFrameEx;
            }
            if (retryIntervals == null) {
                throw subFrameEx;
            }
            if (shouldCancelRetry()) {
                throw subFrameEx;
            }
            long sleepTime = StoreUtil.calcRetryInterval(retryCount, retryIntervals);
            if (sleepTime > 0) {
                log.error("io error, {}th retry scheduled after {} ms", retryCount + 1, sleepTime, subFrameEx);
                this.lastSubFrameEx = subFrameEx;
                return Fiber.sleepUntilShouldStop(sleepTime, this::retry);
            } else {
                log.error("io error, retryCount={}", retryCount, subFrameEx);
                throw subFrameEx;
            }
        }
    }

    private boolean shouldCancelRetry() {
        if (isGroupShouldStopPlain()) {
            log.warn("retry canceled because of fiber group should stop");
            return true;
        }
        if (retryCount >= retryIntervals.length && !retryForever) {
            return true;
        }
        return cancelRetry != null && cancelRetry.get();
    }

    private FrameCallResult retry(Void unused) throws Throwable {
        if (shouldCancelRetry()) {
            throw lastSubFrameEx;
        }
        retryCount++;
        return execute(null);
    }

    @Override
    protected FrameCallResult handle(Throwable ex) throws Throwable {
        if (lastSubFrameEx != null) {
            Throwable root = DtUtil.rootCause(ex);
            if (root instanceof FiberInterruptException || root instanceof FiberCancelException
                    || root instanceof InterruptedException) {
                log.warn("retry canceled because of interrupt or cancel: {}", ex.toString());
                throw lastSubFrameEx;
            }
        }
        throw ex;
    }

}
