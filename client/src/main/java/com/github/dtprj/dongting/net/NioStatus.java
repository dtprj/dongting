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

import com.github.dtprj.dongting.common.IntObjMap;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * @author huangli
 */
class NioStatus {

    @SuppressWarnings("unused")
    long p00, p01, p02, p03, p04, p05, p06, p07, p08, p09, p0a, p0b, p0c, p0d, p0e, p0f;

    // high 24 bits is pending request count, low 40 bits is pending bytes
    private volatile long inPending;

    @SuppressWarnings("unused")
    long p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p1a, p1b, p1c, p1d, p1e, p1f;

    int outPendingRequests;
    long outPendingBytes;

    private final IntObjMap<ReqProcessor<?>> processors = new IntObjMap<>();

    static final AtomicLongFieldUpdater<NioStatus> pendingUpdater =
            AtomicLongFieldUpdater.newUpdater(NioStatus.class, "inPending");

    NioStatus() {
    }

    public ReqProcessor<?> getProcessor(int cmd) {
        return processors.get(cmd);
    }

    public void registerProcessor(int cmd, ReqProcessor<?> processor) {
        processors.put(cmd, processor);
    }

}
