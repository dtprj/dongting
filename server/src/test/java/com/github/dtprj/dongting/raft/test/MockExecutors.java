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
package com.github.dtprj.dongting.raft.test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author huangli
 */
public class MockExecutors {
    private static final ScheduledExecutorService MOCK_IO_EXECUTOR;
    private static final ScheduledExecutorService MOCK_SINGLE_EXECUTOR;

    static {
        MOCK_IO_EXECUTOR = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "MockIOExecutor");
            t.setDaemon(true);
            return t;
        });
        MOCK_SINGLE_EXECUTOR = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "MockSingleExecutor");
            t.setDaemon(true);
            return t;
        });
    }

    public static ScheduledExecutorService ioExecutor() {
        return MOCK_IO_EXECUTOR;
    }

    public static ScheduledExecutorService singleExecutor() {
        return MOCK_SINGLE_EXECUTOR;
    }
}
