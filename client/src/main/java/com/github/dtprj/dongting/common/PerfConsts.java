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
package com.github.dtprj.dongting.common;

/**
 * @author huangli
 */
public interface PerfConsts {
    int PERF_DEBUG1 = -1;
    int PERF_DEBUG2 = -2;
    int PERF_DEBUG3 = -3;

    int RPC_D_ACQUIRE = 1;
    int RPC_D_WORKER_QUEUE = 2;
    int RPC_D_CHANNEL_QUEUE = 3;
    int RPC_D_WORKER_SEL = 4;
    int RPC_D_WORKER_WORK = 5;
    int RPC_C_MARK_READ = 6;
    int RPC_C_MARK_WRITE = 7;
    int RPC_D_READ = 8;
    int RPC_D_WRITE = 9;

    int FIBER_D_POLL = 20;
    int FIBER_D_WORK = 21;

    int RAFT_D_LEADER_RUNNER_FIBER_LATENCY = 30; // cross thread
    int RAFT_D_ENCODE_AND_WRITE = 31;
    int RAFT_D_LOG_WRITE1 = 32;
    int RAFT_D_LOG_WRITE2 = 33;
    int RAFT_D_LOG_SYNC = 34;
    int RAFT_D_IDX_POS_NOT_READY = 35;
    int RAFT_D_LOG_POS_NOT_READY = 36;
    int RAFT_D_IDX_FILE_ALLOC = 37;
    int RAFT_D_LOG_FILE_ALLOC = 38;
    int RAFT_D_IDX_BLOCK = 39;
    int RAFT_D_IDX_WRITE = 40;
    int RAFT_D_IDX_FORCE = 41;
    int RAFT_D_REPLICATE_RPC = 42;
    int RAFT_D_STATE_MACHINE_EXEC = 43; // may fire in other thread

    int DTKV_LEASE_READ = 60;
    int DTKV_LINEARIZABLE_OP = 61;
}
