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
public class DefaultKvPerf extends SimplePerf {

    private final LongAdder leaseReadCount = new LongAdder();
    private final LongAdder leaseReadTotalTime = new LongAdder();
    private final LongAccumulator leaseReadMaxTime = new LongAccumulator(Long::max, 0);
    private final LongAdder linearizableOpCount = new LongAdder();
    private final LongAdder linearizableOpTotalTime = new LongAdder();
    private final LongAccumulator linearizableOpMaxTime = new LongAccumulator(Long::max, 0);

    public DefaultKvPerf() {
        super(false, false);
    }

    @Override
    public boolean accept(int perfType) {
        return perfType == PerfConsts.DTKV_LEASE_READ || perfType == PerfConsts.DTKV_LINEARIZABLE_OP;
    }

    @Override
    public void onEvent(int perfType, long costTime, int count, long sum) {
        if (perfType == PerfConsts.DTKV_LEASE_READ) {
            leaseReadCount.add(count);
            leaseReadTotalTime.add(costTime);
            leaseReadMaxTime.accumulate(costTime);
        } else if (perfType == PerfConsts.DTKV_LINEARIZABLE_OP) {
            linearizableOpCount.add(count);
            linearizableOpTotalTime.add(costTime);
            linearizableOpMaxTime.accumulate(costTime);
        }
    }

    protected void collect() {
        long leaseReadCount = this.leaseReadCount.sumThenReset();
        long leaseReadTotalTime = this.leaseReadTotalTime.sumThenReset();
        long leaseReadMaxTime = this.leaseReadMaxTime.getThenReset();
        long linearizableOpCount = this.linearizableOpCount.sumThenReset();
        long linearizableOpTotalTime = this.linearizableOpTotalTime.sumThenReset();
        long linearizableOpMaxTime = this.linearizableOpMaxTime.getThenReset();
        if (log.isInfoEnabled()) {
            long leaseReadAvg10 = leaseReadCount == 0 ? 0 : leaseReadTotalTime * 10 / leaseReadCount;
            long linearizableOpAvg10 = linearizableOpCount == 0 ? 0 : linearizableOpTotalTime * 10 / linearizableOpCount;
            log.info("kv lease read count: {}, avg time: {}.{}ms, max time: {}ms",
                    leaseReadCount,
                    leaseReadAvg10 / 10,
                    leaseReadAvg10 % 10,
                    leaseReadMaxTime);
            log.info("kv linearizable op count: {}, avg time: {}.{}ms, max time: {}ms",
                    linearizableOpCount,
                    linearizableOpAvg10 / 10,
                    linearizableOpAvg10 % 10,
                    linearizableOpMaxTime);
        }
    }
}
