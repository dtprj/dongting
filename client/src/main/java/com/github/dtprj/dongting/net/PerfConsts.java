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

/**
 * @author huangli
 */
public interface PerfConsts {
    int RPC_D_ACQUIRE = 1;
    int RPC_D_WORKER_QUEUE = 2;
    int RPC_D_CHANNEL_QUEUE = 3;
    int RPC_D_WORKER_SEL = 4;
    int RPC_D_WORKER_WORK = 5;
    int RPC_C_MARK_READ = 6;
    int RPC_C_MARK_WRITE = 7;
    int RPC_D_READ = 8;
    int RPC_D_WRITE = 9;

    int FIBER_D_POLL = 100;
    int FIBER_D_WORK = 101;
}
