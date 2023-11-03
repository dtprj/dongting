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
public abstract class FiberFrame {
    Fiber fiber;
    FiberGroup group;
    FiberFrame prev;

    protected abstract Object execute();

    protected void doFinally() {
    }

    protected Fiber getFiber() {
        return fiber;
    }

    protected FiberGroup getGroup() {
        return group;
    }

    protected final void setOutputObj(Object obj) {
        group.dispatcher.outputObj = obj;
    }

    protected final void setOutputInt(int v) {
        group.dispatcher.outputInt = v;
    }

    protected final void setOutputLong(long v) {
        group.dispatcher.outputLong = v;
    }

    protected final Object getInputObj() {
        return group.dispatcher.inputObj;
    }

    protected final int getInputInt() {
        return group.dispatcher.inputInt;
    }

    protected final long getInputLong() {
        return group.dispatcher.inputLong;
    }
}
