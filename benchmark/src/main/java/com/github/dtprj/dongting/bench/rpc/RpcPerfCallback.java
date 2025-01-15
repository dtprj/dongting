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

import com.github.dtprj.dongting.bench.common.PrometheusPerfCallback;
import io.prometheus.client.Summary;

import java.util.concurrent.atomic.LongAdder;

/**
 * @author huangli
 */
public class RpcPerfCallback extends PrometheusPerfCallback {

    private final Summary rpcAcquire;
    private final Summary rpcWorkerQueue;
    private final Summary rpcChannelQueue;
    private final Summary rpcWorkerSel;
    private final Summary rpcWorkerWork;
    private final LongAdder rpcMarkRead = new LongAdder();
    private final LongAdder rpcMarkWrite = new LongAdder();
    private final Summary rpcReadTime;
    private final Summary rpcReadBytes;
    private final Summary rpcWriteTime;
    private final Summary rpcWriteBytes;

    public RpcPerfCallback(boolean useNanos, String prefix) {
        super(useNanos);
        this.rpcAcquire = createSummary(prefix + "rpc_acquire");
        this.rpcWorkerQueue = createSummary(prefix + "rpc_worker_queue");
        this.rpcChannelQueue = createSummary(prefix + "rpc_channel_queue");
        this.rpcWorkerSel = createSummary(prefix + "rpc_worker_sel");
        this.rpcWorkerWork = createSummary(prefix + "rpc_worker_work");
        this.rpcReadTime = createSummary(prefix + "rpc_read_time");
        this.rpcReadBytes = createSummary(prefix + "rpc_read_bytes");
        this.rpcWriteTime = createSummary(prefix + "rpc_write_time");
        this.rpcWriteBytes = createSummary(prefix + "rpc_write_bytes");
    }

    @Override
    public boolean accept(int perfType) {
        return true;
    }

    @Override
    public void onEvent(int perfType, long costTime, int count, long sum) {
        if (!started) {
            return;
        }
        switch (perfType) {
            case RPC_D_ACQUIRE:
                rpcAcquire.observe(costTime);
                break;
            case RPC_D_WORKER_QUEUE:
                rpcWorkerQueue.observe(costTime);
                break;
            case RPC_D_CHANNEL_QUEUE:
                rpcChannelQueue.observe(costTime);
                break;
            case RPC_D_WORKER_SEL:
                rpcWorkerSel.observe(costTime);
                break;
            case RPC_D_WORKER_WORK:
                rpcWorkerWork.observe(costTime);
                break;
            case RPC_D_READ:
                rpcReadTime.observe(costTime);
                rpcReadBytes.observe(sum);
                break;
            case RPC_D_WRITE:
                rpcWriteTime.observe(costTime);
                rpcWriteBytes.observe(sum);
                break;
            case RPC_C_MARK_READ:
                rpcMarkRead.add(count);
                break;
            case RPC_C_MARK_WRITE:
                rpcMarkWrite.add(count);
                break;
        }
    }

    public void printStats() {
        printTime(rpcAcquire);
        printTime(rpcWorkerQueue);
        printTime(rpcChannelQueue);
        printTime(rpcWorkerSel);
        printTime(rpcWorkerWork);
        printTime(rpcReadTime);
        printValue(rpcReadBytes);
        printTime(rpcWriteTime);
        printValue(rpcWriteBytes);

        printCount("rpc_mark_read", rpcMarkRead);
        printCount("rpc_mark_write", rpcMarkWrite);

        if (accept(RPC_D_WORKER_SEL) && accept(RPC_D_WORKER_WORK)) {
            double total = rpcWorkerSel.get().sum + rpcWorkerWork.get().sum;
            double work = rpcWorkerWork.get().sum / total;
            log.info(String.format("worker thread utilization rate: %.2f%%\n", work * 100));
        }
    }

}
