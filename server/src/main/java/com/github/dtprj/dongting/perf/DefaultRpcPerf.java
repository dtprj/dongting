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
public class DefaultRpcPerf extends SimplePerf {

    private final LongAdder workCount = new LongAdder();
    private final LongAdder workTotalTime = new LongAdder();
    private final LongAccumulator workMaxTime = new LongAccumulator(Long::max, 0);
    private final LongAdder selTotalTime = new LongAdder();

    public DefaultRpcPerf() {
        super(true, false);
    }

    @Override
    public boolean accept(int perfType) {
        return perfType == PerfConsts.RPC_D_WORKER_WORK || perfType == PerfConsts.RPC_D_WORKER_SEL;
    }

    @Override
    public void onEvent(int perfType, long costTime, int count, long sum) {
        if (perfType == PerfConsts.RPC_D_WORKER_WORK) {
            workCount.add(count);
            workTotalTime.add(costTime);
            workMaxTime.accumulate(costTime);
        } else if (perfType == PerfConsts.RPC_D_WORKER_SEL) {
            selTotalTime.add(costTime);
        }
    }

    protected void collect() {
        long workCount = this.workCount.sumThenReset();
        long workTotalTime = this.workTotalTime.sumThenReset();
        long workMaxTime = this.workMaxTime.getThenReset();
        long selTotalTime = this.selTotalTime.sumThenReset();
        if (log.isInfoEnabled()) {
            long avgUs = workCount == 0 ? 0 : workTotalTime / workCount / 1000;
            long totalTime = workTotalTime + selTotalTime;
            long usage10 = totalTime == 0 ? 0 : workTotalTime * 1000 / totalTime;
            log.info("rpc work count: {}, avg time: {}us, max time: {}us, thread avg usage {}.{}%",
                    workCount, avgUs, workMaxTime / 1000, usage10 / 10, usage10 % 10);
        }
    }
}
