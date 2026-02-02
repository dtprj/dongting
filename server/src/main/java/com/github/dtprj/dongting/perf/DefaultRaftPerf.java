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
package com.github.dtprj.dongting.perf;

import com.github.dtprj.dongting.common.PerfConsts;

/**
 * @author huangli
 */
public class DefaultRaftPerf extends SimplePerf {

    private long writeCount;
    private long writeTotalTime;
    private long writeMaxTime;
    private long readCount;

    public DefaultRaftPerf() {
        super(false);
    }

    @Override
    public boolean accept(int perfType) {
        return perfType == PerfConsts.RAFT_D_STATE_MACHINE_EXEC
                || perfType == PerfConsts.RAFT_C_LEASE_READ;
    }

    @Override
    public void onEvent(int perfType, long costTime, int count, long sum) {
        if (perfType == PerfConsts.RAFT_D_STATE_MACHINE_EXEC) {
            writeCount += count;
            writeTotalTime += costTime;
            if (costTime > writeMaxTime) {
                writeMaxTime = costTime;
            }
        } else if (perfType == PerfConsts.RAFT_C_LEASE_READ) {
            readCount += count;
        }

    }

    protected void collect() {
        if (log.isInfoEnabled()) {
            long avgTime1000 = writeCount == 0 ? 0 : writeTotalTime * 1000 / writeCount;
            long frac = avgTime1000 % 1000;
            log.info("raft write count: {}, avg time: {}.{}{}{}ms, max time: {}ms",
                    writeCount,
                    avgTime1000 / 1000,
                    frac / 100,
                    frac % 100 / 10,
                    frac % 10,
                    writeMaxTime);
            log.info("raft read count: {}", readCount);
        }
        writeCount = 0;
        writeTotalTime = 0;
        writeMaxTime = 0;
        readCount = 0;
    }
}
