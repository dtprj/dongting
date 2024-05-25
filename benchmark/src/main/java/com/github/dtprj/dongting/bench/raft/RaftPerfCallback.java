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
package com.github.dtprj.dongting.bench.raft;

import com.github.dtprj.dongting.bench.common.SimplePerfCallback;
import io.prometheus.client.Summary;

/**
 * @author huangli
 */
public class RaftPerfCallback extends SimplePerfCallback {

    private final Summary debugTime;
    private final Summary debugCount;
    private final Summary debugSum;


    private final Summary fiberPoll;
    private final Summary fiberWork;

    private final Summary raftLeaderRunnerFiberLatency;
    private final Summary raftLogWriteFiberRound;
    private final Summary raftLogWriteTime;
    private final Summary raftLogWriteItems;
    private final Summary raftLogWriteBytes;
    private final Summary raftLogSyncTime;
    private final Summary raftLogSyncItems;
    private final Summary raftLogSyncBytes;
    private final Summary raftIdxPosNotReady;
    private final Summary raftLogPosNotReady;
    private final Summary raftIdxFileAlloc;
    private final Summary raftLogFileAlloc;
    private final Summary raftIdxBlock;
    private final Summary raftIdxWriteTime;
    private final Summary raftIdxWriteBytes;
    private final Summary raftIdxWriteAndForceTime;
    private final Summary raftIdxWriteAndForceBytes;
    private final Summary raftReplicateRpcTime;
    private final Summary raftReplicateRpcItems;
    private final Summary raftReplicateRpcBytes;

    public RaftPerfCallback(boolean useNanos, String prefix) {
        super(useNanos);
        this.debugTime = createSummary(prefix + "debug_time");
        this.debugCount = createSummary(prefix + "debug_count");
        this.debugSum = createSummary(prefix + "debug_sum");
        this.fiberPoll = createSummary(prefix + "fiber_poll");
        this.fiberWork = createSummary(prefix + "fiber_work");
        this.raftLeaderRunnerFiberLatency = createSummary(prefix + "raft_leader_runner_fiber_latency");
        this.raftLogWriteFiberRound = createSummary(prefix + "raft_log_write_fiber_round");
        this.raftLogWriteTime = createSummary(prefix + "raft_log_write_time");
        this.raftLogWriteItems = createSummary(prefix + "raft_log_write_items");
        this.raftLogWriteBytes = createSummary(prefix + "raft_log_write_bytes");
        this.raftLogSyncTime = createSummary(prefix + "raft_log_sync_time");
        this.raftLogSyncItems = createSummary(prefix + "raft_log_sync_items");
        this.raftLogSyncBytes = createSummary(prefix + "raft_log_sync_bytes");
        this.raftIdxPosNotReady = createSummary(prefix + "raft_idx_pos_not_ready");
        this.raftLogPosNotReady = createSummary(prefix + "raft_log_pos_not_ready");
        this.raftIdxFileAlloc = createSummary(prefix + "raft_idx_file_alloc");
        this.raftLogFileAlloc = createSummary(prefix + "raft_log_file_alloc");
        this.raftIdxBlock = createSummary(prefix + "raft_idx_block");
        this.raftIdxWriteTime = createSummary(prefix + "raft_idx_write_time");
        this.raftIdxWriteBytes = createSummary(prefix + "raft_idx_write_bytes");
        this.raftIdxWriteAndForceTime = createSummary(prefix + "raft_idx_write_and_force_time");
        this.raftIdxWriteAndForceBytes = createSummary(prefix + "raft_idx_write_and_force_bytes");
        this.raftReplicateRpcTime = createSummary(prefix + "raft_replicate_rpc_time");
        this.raftReplicateRpcItems = createSummary(prefix + "raft_replicate_rpc_items");
        this.raftReplicateRpcBytes = createSummary(prefix + "raft_replicate_rpc_bytes");
    }

    @Override
    public boolean accept(int perfType) {
        return true;
    }

    @Override
    public void onEvent(int perfType, long costTime, int count, long sum) {
        switch (perfType) {
            case PERF_DEBUG:
                debugTime.observe(costTime);
                debugCount.observe(count);
                debugSum.observe(sum);
                break;
            case FIBER_D_POLL:
                fiberPoll.observe(costTime);
                break;
            case FIBER_D_WORK:
                fiberWork.observe(costTime);
                break;
            case RAFT_D_LEADER_RUNNER_FIBER_LATENCY:
                raftLeaderRunnerFiberLatency.observe(costTime);
                break;
            case RAFT_D_LOG_WRITE_FIBER_ROUND:
                raftLogWriteFiberRound.observe(costTime);
                break;
            case RAFT_D_LOG_WRITE:
                raftLogWriteTime.observe(costTime);
                raftLogWriteItems.observe(count);
                raftLogWriteBytes.observe(sum);
                break;
            case RAFT_D_LOG_SYNC:
                raftLogSyncTime.observe(costTime);
                raftLogSyncItems.observe(count);
                raftLogSyncBytes.observe(sum);
                break;
            case RAFT_D_IDX_POS_NOT_READY:
                raftIdxPosNotReady.observe(costTime);
                break;
            case RAFT_D_LOG_POS_NOT_READY:
                raftLogPosNotReady.observe(costTime);
                break;
            case RAFT_D_IDX_FILE_ALLOC:
                raftIdxFileAlloc.observe(costTime);
                break;
            case RAFT_D_LOG_FILE_ALLOC:
                raftLogFileAlloc.observe(costTime);
                break;
            case RAFT_D_IDX_BLOCK:
                raftIdxBlock.observe(costTime);
                break;
            case RAFT_D_IDX_WRITE:
                raftIdxWriteTime.observe(costTime);
                raftIdxWriteBytes.observe(sum);
                break;
            case RAFT_D_IDX_WRITE_AND_FORCE:
                raftIdxWriteAndForceTime.observe(costTime);
                raftIdxWriteAndForceBytes.observe(sum);
                break;
            case RAFT_D_REPLICATE_RPC:
                raftReplicateRpcTime.observe(costTime);
                raftReplicateRpcItems.observe(count);
                raftReplicateRpcBytes.observe(sum);
                break;
        }
    }

    public void printStats() {
        printTime(debugTime);
        printValue(debugCount);
        printValue(debugSum);
        printTime(fiberPoll);
        printTime(fiberWork);
        printTime(raftLeaderRunnerFiberLatency);
        printTime(raftLogWriteFiberRound);
        printTime(raftLogWriteTime);
        printValue(raftLogWriteItems);
        printValue(raftLogWriteBytes);
        printTime(raftLogSyncTime);
        printValue(raftLogSyncItems);
        printValue(raftLogSyncBytes);
        printTime(raftIdxPosNotReady);
        printTime(raftLogPosNotReady);
        printTime(raftIdxFileAlloc);
        printTime(raftLogFileAlloc);
        printTime(raftIdxBlock);
        printTime(raftIdxWriteTime);
        printValue(raftIdxWriteBytes);
        printTime(raftIdxWriteAndForceTime);
        printValue(raftIdxWriteAndForceBytes);
        printTime(raftReplicateRpcTime);
        printValue(raftReplicateRpcItems);
        printValue(raftReplicateRpcBytes);

        if (accept(FIBER_D_POLL) && accept(FIBER_D_WORK)) {
            double total = fiberPoll.get().sum + fiberWork.get().sum;
            double work = fiberWork.get().sum / total;
            System.out.printf("fiber thread utilization rate: %.2f%%\n", work * 100);
        }
    }

}
