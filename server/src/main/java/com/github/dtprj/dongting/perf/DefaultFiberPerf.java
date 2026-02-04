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
public class DefaultFiberPerf extends SimplePerf {

    private long workCount;
    private long workTotalTime;
    private long workMaxTime;
    private long pollTotalTime;

    public DefaultFiberPerf() {
        super(true, true);
    }

    @Override
    public boolean accept(int perfType) {
        return perfType == PerfConsts.FIBER_D_POLL || perfType == PerfConsts.FIBER_D_WORK;
    }

    @Override
    public void onEvent(int perfType, long costTime, int count, long sum) {
        if (perfType == PerfConsts.FIBER_D_WORK) {
            workCount += count;
            workTotalTime += costTime;
            if (costTime > workMaxTime) {
                workMaxTime = costTime;
            }
        } else if (perfType == PerfConsts.FIBER_D_POLL) {
            pollTotalTime += costTime;
        }
    }

    protected void collect() {
        if (log.isInfoEnabled()) {
            long totalTime = workTotalTime + pollTotalTime;
            long usage10 = totalTime == 0 ? 0 : workTotalTime * 1000 / totalTime;
            log.info("fiber work count {}, avg time: {}us, max time: {}us, thread usage {}.{}%",
                    workCount,
                    workCount == 0 ? 0 : workTotalTime / workCount / 1000,
                    workMaxTime / 1000,
                    usage10 / 10,
                    usage10 % 10);
        }
        workCount = 0;
        workTotalTime = 0;
        workMaxTime = 0;
        pollTotalTime = 0;
    }

}
