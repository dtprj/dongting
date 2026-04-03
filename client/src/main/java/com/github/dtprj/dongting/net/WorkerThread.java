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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.common.DtThread;
import com.github.dtprj.dongting.common.Timestamp;

/**
 * @author huangli
 */
public final class WorkerThread extends DtThread {

    public final Timestamp ts;
    public final DecodeContext decodeContext;

    WorkerThread(Runnable r, String name, Timestamp ts, RefBufferFactory refBufferFactory) {
        super(r, name);
        this.ts = ts;
        this.decodeContext = DecodeContext.factory.apply(refBufferFactory, threadLocalBuffer);
    }

    public static WorkerThread currentWorkerThread() {
        Thread t = Thread.currentThread();
        if (t instanceof WorkerThread) {
            return (WorkerThread) t;
        }
        throw new NetException("not in WorkerThread");
    }
}
