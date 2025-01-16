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
package com.github.dtprj.dongting.bench.common;

import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author huangli
 */
public class SimplePerfCallback extends PerfCallback {
    private static final DtLog log = DtLogs.getLogger(SimplePerfCallback.class);

    private final ConcurrentHashMap<Integer, Value> map = new ConcurrentHashMap<>();
    private final String prefix;

    protected volatile boolean started = false;

    public SimplePerfCallback(String prefix) {
        super(true);
        this.prefix = prefix;
    }

    static class Value {
        final String name;
        final LongAdder invokeCount = new LongAdder();
        final LongAdder count = new LongAdder();
        final LongAdder sum = new LongAdder();
        final LongAdder time = new LongAdder();
        final AtomicLong maxCount = new AtomicLong(Long.MIN_VALUE);
        final AtomicLong maxTime = new AtomicLong(Long.MIN_VALUE);
        final AtomicLong maxSum = new AtomicLong(Long.MIN_VALUE);

        Value(String name) {
            this.name = name;
        }
    }

    @Override
    public boolean accept(int perfType) {
        return started;
    }

    @Override
    public void onEvent(int perfType, long costTime, int count, long sum) {
        if (!started) {
            return;
        }
        Value value = map.computeIfAbsent(perfType, k -> new Value(getName(perfType)));
        value.invokeCount.add(1);
        value.count.add(count);
        value.time.add(costTime);
        value.sum.add(sum);
        AtomicLong x = value.maxTime;
        long v;
        while (costTime > (v = x.longValue())) {
            if (x.compareAndSet(v, costTime)) {
                break;
            }
        }
        x = value.maxCount;
        while (costTime > (v = x.longValue())) {
            if (x.compareAndSet(v, count)) {
                break;
            }
        }
        x = value.maxSum;
        while (costTime > (v = x.longValue())) {
            if (x.compareAndSet(v, sum)) {
                break;
            }
        }
    }

    protected String getName(int perfType) {
        return switch (perfType) {
            case PERF_DEBUG1 -> prefix + "debug1";
            case PERF_DEBUG2 -> prefix + "debug2";
            case PERF_DEBUG3 -> prefix + "debug3";
            case RPC_D_ACQUIRE -> prefix + "rpc_acquire";
            case RPC_D_WORKER_QUEUE -> prefix + "rpc_worker_queue";
            case RPC_D_CHANNEL_QUEUE -> prefix + "rpc_channel_queue";
            case RPC_D_WORKER_SEL -> prefix + "rpc_worker_sel";
            case RPC_D_WORKER_WORK -> prefix + "rpc_worker_work";
            case RPC_C_MARK_READ -> prefix + "rpc_mark_read";
            case RPC_C_MARK_WRITE -> prefix + "rpc_mark_write";
            case RPC_D_READ -> prefix + "rpc_read";
            case RPC_D_WRITE -> prefix + "rpc_write";
            case FIBER_D_POLL -> prefix + "fiber_poll";
            case FIBER_D_WORK -> prefix + "fiber_work";
            case RAFT_D_LEADER_RUNNER_FIBER_LATENCY -> prefix + "raft_leader_runner_fiber_latency";
            case RAFT_D_ENCODE_AND_WRITE -> prefix + "raft_encode_and_write";
            case RAFT_D_LOG_WRITE1 -> prefix + "raft_log_write1";
            case RAFT_D_LOG_WRITE2 -> prefix + "raft_log_write2";
            case RAFT_D_LOG_SYNC -> prefix + "raft_log_sync";
            case RAFT_D_IDX_POS_NOT_READY -> prefix + "raft_idx_pos_not_ready";
            case RAFT_D_LOG_POS_NOT_READY -> prefix + "raft_log_pos_not_ready";
            case RAFT_D_IDX_FILE_ALLOC -> prefix + "raft_idx_file_alloc";
            case RAFT_D_LOG_FILE_ALLOC -> prefix + "raft_log_file_alloc";
            case RAFT_D_IDX_BLOCK -> prefix + "raft_idx_block";
            case RAFT_D_IDX_WRITE -> prefix + "raft_idx_write";
            case RAFT_D_IDX_FORCE -> prefix + "raft_idx_force";
            case RAFT_D_REPLICATE_RPC -> prefix + "raft_replicate_rpc";
            case RAFT_D_STATE_MACHINE_EXEC -> prefix + "raft_state_machine_exec";
            default -> prefix + "unknown";
        };
    }

    public void start() {
        started = true;
    }

    public void printStats() {
        map.forEach((perfType, value) -> {
            long invokeCount = value.invokeCount.sum();
            if (invokeCount == 0) {
                return;
            }
            long totalCount = value.count.sum();
            long totalSum = value.sum.sum();
            long totalTime = value.time.sum();
            double avgTime = totalTime / (double) invokeCount;
            double avgSum = totalSum / (double) invokeCount;
            double avgCount = totalCount / (double) invokeCount;
            String s = String.format("%s: call %,d, avgTime %,.3fus, totalTime %,.1fms, avgSum %,.1f, avgCount %,.1f," +
                            " maxTime %,.3fus, maxSum %,d, maxCount %,d",
                    value.name, invokeCount, avgTime / 1000, totalTime / 1_000_000.0, avgSum, avgCount,
                    value.maxTime.doubleValue() / 1000, value.maxSum.longValue(), value.maxCount.longValue());

            log.info(s);
        });
    }
}
