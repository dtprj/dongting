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
public class FiberTestUtil {

    public static final Dispatcher DISPATCHER;
    public static final FiberGroup FIBER_GROUP;

    static {
        try {
            DISPATCHER = new Dispatcher("test");
            DISPATCHER.start();
            FIBER_GROUP = new FiberGroup("test group", DISPATCHER);
            DISPATCHER.startGroup(FIBER_GROUP).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
