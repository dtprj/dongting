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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

/**
 * @author huangli
 */
abstract class FiberQueueTask {
    private static final DtLog log = DtLogs.getLogger(FiberQueueTask.class);

    boolean failIfGroupShouldStop;

    final FiberGroup ownerGroup;

    FiberQueueTask next;

    public FiberQueueTask(FiberGroup ownerGroup) {
        this.ownerGroup = ownerGroup;
    }

    protected abstract void run();

    protected void onDispatchFail(){
    }

    boolean dispatchCheck() {
        if (failIfGroupShouldStop && ownerGroup.isShouldStopPlain()) {
            try {
                log.warn("task is not accepted because its group is shouldStop: {}", this);
                onDispatchFail();
            } catch (Throwable e) {
                log.error("", e);
            }
            return false;
        }
        return true;
    }
}
