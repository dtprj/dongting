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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.common.Timestamp;

/**
 * @author huangli
 */
class WorkerStatus {
    final NioWorker worker;
    final IoWorkerQueue ioWorkerQueue;
    final LongObjMap<WriteData> pendingRequests;
    final ByteBufferPool directPool;
    final RefBufferFactory heapPool;
    final Timestamp ts;

    int retryConnect;

    int packetsToWrite;

    public WorkerStatus(NioWorker worker, IoWorkerQueue ioWorkerQueue,
                        LongObjMap<WriteData> pendingRequests, ByteBufferPool directPool,
                        RefBufferFactory heapPool, Timestamp ts) {
        this.worker = worker;
        this.ioWorkerQueue = ioWorkerQueue;
        this.pendingRequests = pendingRequests;
        this.directPool = directPool;
        this.heapPool = heapPool;
        this.ts = ts;
    }

    public void addPacketsToWrite(int delta) {
        this.packetsToWrite = Math.max(packetsToWrite + delta, 0);
    }
}
