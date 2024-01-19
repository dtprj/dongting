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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;

/**
 * @author huangli
 */
class ReplicateStatus {
    private final FiberCondition finishCondition;
    private int epoch;
    private Fiber replicateFiber;

    ReplicateStatus(FiberCondition finishCondition) {
        this.finishCondition = finishCondition;
    }

    public int getEpoch() {
        return epoch;
    }

    public void incrementEpoch(int oldEpoch) {
        if (epoch == oldEpoch) {
            epoch++;
        }
    }

    public FiberCondition getFinishCondition() {
        return finishCondition;
    }

    public Fiber getReplicateFiber() {
        return replicateFiber;
    }

    public void setReplicateFiber(Fiber replicateFiber) {
        this.replicateFiber = replicateFiber;
    }
}
