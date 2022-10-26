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

import com.carrotsearch.hppc.LongObjectHashMap;

/**
 * @author huangli
 */
class WorkerParams {
    private IoQueue ioQueue;
    private Runnable wakeupRunnable;
    private LongObjectHashMap<WriteData> pendingRequests;
    private ByteBufferPool pool;

    public WorkerParams() {
    }

    public IoQueue getIoQueue() {
        return ioQueue;
    }

    public void setIoQueue(IoQueue ioQueue) {
        this.ioQueue = ioQueue;
    }

    public Runnable getWakeupRunnable() {
        return wakeupRunnable;
    }

    public void setWakeupRunnable(Runnable wakeupRunnable) {
        this.wakeupRunnable = wakeupRunnable;
    }

    public  LongObjectHashMap<WriteData> getPendingRequests() {
        return pendingRequests;
    }

    public void setPendingRequests( LongObjectHashMap<WriteData> pendingRequests) {
        this.pendingRequests = pendingRequests;
    }

    public ByteBufferPool getPool() {
        return pool;
    }

    public void setPool(ByteBufferPool pool) {
        this.pool = pool;
    }

}
