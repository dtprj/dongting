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
package com.github.dtprj.dongting.fiber;

/**
 * @author huangli
 */
public class FiberCondition extends WaitSource {

    public FiberCondition(FiberGroup group) {
        super(group);
    }

    @Override
    protected boolean shouldWait(Fiber currentFiber) {
        return true;
    }

    public void signal() {
        if (group.isInGroupThread()) {
            signal0();
        } else {
            throw new FiberException("signal can only call in group thread");
        }
    }

    public void signalAll() {
        if (group.isInGroupThread()) {
            signalAll0();
        } else {
            throw new FiberException("signal can only call in group thread");
        }
    }
}
