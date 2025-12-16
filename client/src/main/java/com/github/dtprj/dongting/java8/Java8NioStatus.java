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
package com.github.dtprj.dongting.java8;

import com.github.dtprj.dongting.net.NioStatus;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * @author huangli
 */
public class Java8NioStatus extends NioStatus {
    // high 24 bits is pending request count, low 40 bits is pending bytes
    protected volatile long inPending;

    private static final AtomicLongFieldUpdater<Java8NioStatus> IN_PENDING =
            AtomicLongFieldUpdater.newUpdater(Java8NioStatus.class, "inPending");

    @Override
    protected long getAndAddRelease(long delta) {
        return IN_PENDING.getAndAdd(this, delta);
    }
}
