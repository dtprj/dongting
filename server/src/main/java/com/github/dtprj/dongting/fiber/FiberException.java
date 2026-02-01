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

import com.github.dtprj.dongting.common.DtException;

/**
 * @author huangli
 */
public class FiberException extends DtException {
    private static final long serialVersionUID = 7000896886981197542L;

    public FiberException() {
        super();
        addVirtualStackTrace(this);
    }

    public FiberException(String message) {
        super(message);
        addVirtualStackTrace(this);
    }

    public FiberException(String message, Throwable cause) {
        super(message, cause);
        addVirtualStackTrace(this);
    }

    private static void addVirtualStackTrace(Throwable ex) {
        Thread t = Thread.currentThread();
        if (!(t instanceof DispatcherThread)) {
            return;
        }
        DispatcherThread dt = (DispatcherThread) t;
        if (dt.currentGroup == null) {
            return;
        }
        Fiber f = dt.currentGroup.currentFiber;
        addVirtualStackTrace(ex, f);
    }

    static void addVirtualStackTrace(Throwable ex, Fiber f) {
        if (f == null) {
            return;
        }
        FiberFrame<?> ff = f.stackTop;
        if (ff == null) {
            return;
        }
        int frameCount = 0;
        while (ff != null) {
            frameCount++;
            ff = ff.prev;
        }
        ff = f.stackTop;
        StackTraceElement[] stackTrace = new StackTraceElement[frameCount];
        for (int i = 0; i < frameCount; i++) {
            stackTrace[i] = new StackTraceElement(ff.getClass().getName(), "N/A", f.name, 0);
            ff = ff.prev;
        }
        FiberVirtualException fe = new FiberVirtualException();
        fe.setStackTrace(stackTrace);
        ex.addSuppressed(fe);
    }
}

class FiberVirtualException extends Throwable {
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
