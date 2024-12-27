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

import com.github.dtprj.dongting.common.RunnableEx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * This class used for biz tests.
 *
 * @author huangli
 */
public class BaseFiberTest {
    protected static Dispatcher dispatcher;
    protected static FiberGroup fiberGroup;

    @BeforeAll
    public static void initGroup() throws Exception {
        dispatcher = new Dispatcher("TestDispatcher");
        dispatcher.start();
        fiberGroup = new FiberGroup("TestGroup", dispatcher);
        dispatcher.startGroup(fiberGroup).get();
    }

    @AfterAll
    public static void shutdownDispatcher() throws Exception {
        AbstractFiberTest.shutdownDispatcher(dispatcher, fiberGroup);
    }

    protected void doInFiber(FiberFrame<Void> fiberFrame) throws Exception {
        AbstractFiberTest.doInFiber(fiberGroup, fiberFrame);
    }

    protected void doInFiber(RunnableEx<Throwable> callback) throws Exception {
        AbstractFiberTest.doInFiber(fiberGroup, callback);
    }
}
