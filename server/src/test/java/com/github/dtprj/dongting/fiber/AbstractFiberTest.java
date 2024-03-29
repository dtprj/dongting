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

import com.github.dtprj.dongting.common.DtTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
class AbstractFiberTest {
    protected Dispatcher dispatcher;
    protected FiberGroup fiberGroup;

    @BeforeEach
    public void initGroup() throws Exception {
        dispatcher = new Dispatcher("test");
        dispatcher.start();
        fiberGroup = new FiberGroup("test group", dispatcher);
        dispatcher.startGroup(fiberGroup).get();
    }

    @AfterEach
    public void shutdownDispatcher() throws Exception {
        if (dispatcher.thread.isAlive()) {
            dispatcher.stop(new DtTime(1000, TimeUnit.MILLISECONDS));
            dispatcher.thread.join(1500);
            Assertions.assertFalse(dispatcher.thread.isAlive());
            assertTrue(fiberGroup.finished);
        }
    }
}
