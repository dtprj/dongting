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
package com.github.dtprj.dongting.bench.rpc;

import com.github.dtprj.dongting.common.PerfCallback;

/**
 * @author huangli
 */
public class RpcPerfCallback extends PerfCallback {
    private long rpcAcquireCount;
    private long rpcAcquireTime;
    private long rpcWorkerQueueCount;
    private long rpcWorkerQueueTime;
    private long rpcChannelQueueCount;
    private long rpcChannelQueueTime;

    public RpcPerfCallback(boolean useNanos) {
        super(useNanos);
    }

    @Override
    protected boolean accept(int perfType) {
        return true;
    }

    @Override
    protected void duration(int perfType, long costTime) {
        switch (perfType) {
            case PerfCallback.RPC_ACQUIRE:
                rpcAcquireCount++;
                rpcAcquireTime += costTime;
                break;
            case PerfCallback.RPC_WORKER_QUEUE:
                rpcWorkerQueueCount++;
                rpcWorkerQueueTime += costTime;
                break;
            case PerfCallback.RPC_CHANNEL_QUEUE:
                rpcChannelQueueCount++;
                rpcChannelQueueTime += costTime;
                break;
        }
    }

    public void printStats() {
        print("rpcAcquireCount", rpcAcquireCount, rpcAcquireTime);
        print("rpcWorkerQueueCount", rpcWorkerQueueCount, rpcWorkerQueueTime);
        print("rpcChannelQueueCount", rpcChannelQueueCount, rpcChannelQueueTime);
    }

    protected void print(String name, long count, long totalTime) {
        if (count == 0) {
            return;
        }
        double avg = 1.0 * totalTime / count;
        if (useNanos) {
            System.out.printf("%s: %d, avg: %,.1fus\n", name, count, avg / 1000);
        } else {
            System.out.printf("%s: %d, avg: %.1fms\n", name, count, avg);
        }
    }
}
