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
package com.github.dtprj.dongting.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author huangli
 */
public class MockDtTime extends DtTime {

    private final AtomicBoolean mockTimeout = new AtomicBoolean(false);

    public MockDtTime() {
    }

    public MockDtTime(long timeout, TimeUnit unit) {
        super(timeout, unit);
    }

    public MockDtTime(Timestamp ts, long timeout, TimeUnit unit) {
        super(ts, timeout, unit);
    }

    public MockDtTime(long nanoTime, long timeout, TimeUnit unit) {
        super(nanoTime, timeout, unit);
    }

    public void markTimeout() {
        mockTimeout.set(true);
    }

    @Override
    public boolean isTimeout() {
        return mockTimeout.get();
    }

    @Override
    public boolean isTimeout(Timestamp ts) {
        return mockTimeout.get();
    }
}
