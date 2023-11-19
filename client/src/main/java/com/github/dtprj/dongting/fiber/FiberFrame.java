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
package com.github.dtprj.dongting.fiber;

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

/**
 * @author huangli
 */
@SuppressWarnings("rawtypes")
public abstract class FiberFrame<O> implements FrameCall<Void> {
    private static final DtLog log = DtLogs.getLogger(FiberFrame.class);
    static final int STATUS_INIT = 0;
    static final int STATUS_BODY_CALLED = 1;
    static final int STATUS_CATCH_CALLED = 2;
    static final int STATUS_FINALLY_CALLED = 3;
    Fiber fiber;
    FiberGroup fiberGroup;
    FiberFrame prev;
    int status;

    FrameCall resumePoint;

    O result;

    protected FrameCallResult doFinally() {
        return FrameCallResult.RETURN;
    }

    protected FrameCallResult handle(Throwable ex) throws Throwable {
        throw ex;
    }

    void reset(Fiber f) {
        if (this.status != STATUS_INIT && this.status != STATUS_FINALLY_CALLED) {
            throw new FiberException("frame is in use");
        }
        this.fiber = f;
        this.fiberGroup = f.fiberGroup;
        this.prev = null;
        this.status = STATUS_INIT;
        this.resumePoint = null;
        this.result = null;
    }


    protected <O2> FrameCallResult call(FiberFrame<O2> subFrame, FrameCall<O2> resumePoint) {
        fiberGroup.dispatcher.call(this, subFrame, resumePoint);
        return FrameCallResult.CALL_NEXT_FRAME;
    }

    protected FrameCallResult awaitOn(FiberCondition c, FrameCall<Void> resumePoint) {
        return fiberGroup.dispatcher.awaitOn(this, c, 0, resumePoint);
    }

    protected FrameCallResult awaitOn(FiberCondition c, long millis, FrameCall<Void> resumePoint) {
        return fiberGroup.dispatcher.awaitOn(this, c, millis, resumePoint);
    }

    protected <T> FrameCallResult awaitOn(FiberFuture<T> f, FrameCall<T> resumePoint) {
        return fiberGroup.dispatcher.awaitOn(this, f, 0, resumePoint);
    }

    protected <T> FrameCallResult awaitOn(FiberFuture<T> f, long millis, FrameCall<T> resumePoint) {
        return fiberGroup.dispatcher.awaitOn(this, f, millis, resumePoint);
    }

    protected FrameCallResult lock(FiberLock lock, FrameCall<Void> resumePoint) {
        return lock.lock(this, 0, resumePoint);
    }

    protected FrameCallResult lock(FiberLock lock, long millis, FrameCall<Void> resumePoint) {
        return lock.lock(this, millis, resumePoint);
    }

    protected FrameCallResult sleep(long millis, FrameCall<Void> resumePoint) {
        fiberGroup.dispatcher.sleep(this, millis, resumePoint);
        return FrameCallResult.SUSPEND;
    }

    protected FrameCallResult fatal(Throwable ex) throws Throwable {
        log.error("encountered fatal error, raft group will shutdown", ex);
        fiberGroup.requestShutdown();
        throw ex;
    }

    protected boolean isGroupShouldStop() {
        return fiberGroup.shouldStop;
    }

    protected FrameCallResult frameReturn() {
        return FrameCallResult.RETURN;
    }

    protected void setResult(O result){
        this.result = result;
    }

    protected <T> FiberChannel<T> createOrGetChannel(int type) {
        return fiberGroup.createOrGetChannel(type);
    }

    protected Fiber getFiber() {
        return fiber;
    }

    protected FiberGroup getFiberGroup() {
        return fiberGroup;
    }

    protected boolean finished(Fiber fiber) {
        return fiber.finished;
    }
}
