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

import java.util.concurrent.atomic.LongAdder;

/**
 * @author huangli
 */
public class RpcPerfCallback extends PerfCallback {
    private final LongAdder rpcAcquireCount = new LongAdder();
    private final LongAdder rpcAcquireTime = new LongAdder();
    private final LongAdder rpcWorkerQueueCount = new LongAdder();
    private final LongAdder rpcWorkerQueueTime = new LongAdder();
    private final LongAdder rpcChannelQueueCount = new LongAdder();
    private final LongAdder rpcChannelQueueTime = new LongAdder();

    public RpcPerfCallback(boolean useNanos) {
        super(useNanos);
    }

    @Override
    public boolean accept(int perfType) {
        return true;
    }

    @Override
    public void onDuration(int perfType, long costTime, long value) {
        switch (perfType) {
            case PerfCallback.D_RPC_ACQUIRE:
                rpcAcquireCount.increment();
                rpcAcquireTime.add(costTime);
                break;
            case PerfCallback.D_RPC_WORKER_QUEUE:
                rpcWorkerQueueCount.increment();
                rpcWorkerQueueTime.add(costTime);
                break;
            case PerfCallback.D_RPC_CHANNEL_QUEUE:
                rpcChannelQueueCount.increment();
                rpcChannelQueueTime.add(costTime);
                break;
        }
    }

    @Override
    public void onCount(int perfType, long value) {
    }

    public void printStats() {
        print("rpcAcquire", rpcAcquireCount.sum(), rpcAcquireTime.sum());
        print("rpcWorkerQueue", rpcWorkerQueueCount.sum(), rpcWorkerQueueTime.sum());
        print("rpcChannelQueue", rpcChannelQueueCount.sum(), rpcChannelQueueTime.sum());
    }

    protected void print(String name, long count, long totalTime) {
        if (count == 0) {
            return;
        }
        double avg = 1.0 * totalTime / count;
        if (useNanos) {
            System.out.printf("%s: %d, avgTime: %,.3fus\n", name, count, avg / 1000);
        } else {
            System.out.printf("%s: %d, avgTime: %.1fms\n", name, count, avg);
        }
    }
}
