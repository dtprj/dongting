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
    IoWorkerQueue ioWorkerQueue;
    Runnable wakeupRunnable;
    LongObjMap<WriteData> pendingRequests;
    ByteBufferPool directPool;
    RefBufferFactory heapPool;
    int packetsToWrite;
    Timestamp ts;

    int retryConnect;

    public WorkerStatus(NioWorker worker) {
        this.worker = worker;
    }

    public NioWorker getWorker() {
        return worker;
    }

    public IoWorkerQueue getIoQueue() {
        return ioWorkerQueue;
    }

    public void setIoQueue(IoWorkerQueue ioWorkerQueue) {
        this.ioWorkerQueue = ioWorkerQueue;
    }

    public Runnable getWakeupRunnable() {
        return wakeupRunnable;
    }

    public void setWakeupRunnable(Runnable wakeupRunnable) {
        this.wakeupRunnable = wakeupRunnable;
    }

    public LongObjMap<WriteData> getPendingRequests() {
        return pendingRequests;
    }

    public void setPendingRequests(LongObjMap<WriteData> pendingRequests) {
        this.pendingRequests = pendingRequests;
    }

    public ByteBufferPool getDirectPool() {
        return directPool;
    }

    public void setDirectPool(ByteBufferPool directPool) {
        this.directPool = directPool;
    }

    public RefBufferFactory getHeapPool() {
        return heapPool;
    }

    public void setHeapPool(RefBufferFactory heapPool) {
        this.heapPool = heapPool;
    }

    public int getPacketsToWrite() {
        return packetsToWrite;
    }

    public void addPacketsToWrite(int delta) {
        this.packetsToWrite = Math.max(packetsToWrite + delta, 0);
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }
}
