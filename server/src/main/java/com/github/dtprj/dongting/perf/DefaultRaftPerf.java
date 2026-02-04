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

import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author huangli
 */
public class DefaultRaftPerf extends SimplePerf {

    private final LongAdder writeCount = new LongAdder();
    private final LongAdder writeTotalTime = new LongAdder();
    private final LongAccumulator writeMaxTime = new LongAccumulator(Long::max, 0);

    public DefaultRaftPerf() {
        // the RAFT_D_STATE_MACHINE_EXEC may fire in separate executor, so can't use collector executor
        super(false, false);
    }

    @Override
    public boolean accept(int perfType) {
        return perfType == PerfConsts.RAFT_D_STATE_MACHINE_EXEC;
    }

    @Override
    public void onEvent(int perfType, long costTime, int count, long sum) {
        if (perfType == PerfConsts.RAFT_D_STATE_MACHINE_EXEC) {
            writeCount.add(count);
            writeTotalTime.add(costTime);
            writeMaxTime.accumulate(costTime);
        }
    }

    @Override
    protected void collect() {
        long writeCount = this.writeCount.sumThenReset();
        long writeTotalTime = this.writeTotalTime.sumThenReset();
        long writeMaxTime = this.writeMaxTime.getThenReset();
        if (log.isInfoEnabled()) {
            long avgTime100 = writeCount == 0 ? 0 : writeTotalTime * 100 / writeCount;
            long frac = avgTime100 % 100;
            log.info("raft state machine exec count: {}, avg time: {}.{}{}ms, max time: {}ms",
                    writeCount,
                    avgTime100 / 100,
                    frac / 10,
                    frac % 10,
                    writeMaxTime);
        }
    }
}
