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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCancelException;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberInterruptException;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;

/**
 * @author huangli
 */
public class IoRetryFrame<O> extends FiberFrame<O> {

    private static final DtLog log = DtLogs.getLogger(IoRetryFrame.class);

    private final long[] retryInterval;
    private final FiberFrame<O> subFrame;
    private final boolean retryForever;
    private int retryCount;

    public IoRetryFrame(FiberFrame<O> subFrame, long[] retryInterval, boolean retryForever) {
        this.retryInterval = retryInterval;
        this.subFrame = subFrame;
        this.retryForever = retryForever;
    }

    @Override
    public FrameCallResult execute(Void input) {
        if (retryCount > 0 && isGroupShouldStopPlain()) {
            throw new RaftException("group should stop, stop retry");
        }
        return Fiber.call(subFrame, this::resume);
    }

    private FrameCallResult resume(O o) {
        setResult(o);
        return Fiber.frameReturn();
    }

    @Override
    protected FrameCallResult handle(Throwable ex) throws Throwable {
        if (isGroupShouldStopPlain()) {
            throw new RaftException("group should stop, stop retry", ex);
        }
        Throwable root = DtUtil.rootCause(ex);
        if (root instanceof FiberInterruptException || root instanceof FiberCancelException) {
            throw ex;
        }
        if (retryInterval == null || retryInterval.length == 0) {
            throw ex;
        }
        long sleepTime;
        if (retryCount >= retryInterval.length) {
            if (!retryForever) {
                throw ex;
            }
            sleepTime = retryInterval[retryInterval.length - 1];
            retryCount++;
        } else {
            sleepTime = retryInterval[retryCount++];
        }
        log.error("io error, retry after {} ms", sleepTime, ex);
        return Fiber.sleepUntilShouldStop(sleepTime, this);
    }

    @Override
    protected FrameCallResult doFinally() {
        retryCount = 0;
        return Fiber.frameReturn();
    }
}
