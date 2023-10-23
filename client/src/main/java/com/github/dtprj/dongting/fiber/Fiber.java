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
public abstract class Fiber {

    private final boolean daemon;
    private Dispatcher dispatcher;
    private boolean ready;

    public Fiber(boolean daemon) {
        this.daemon = daemon;
    }

    public abstract FiberEntryPoint nextEntryPoint();

    public boolean isDaemon() {
        return daemon;
    }

    protected void finish() {
        dispatcher.finish(this);
    }

    protected void awaitOn(Condition c) {
        c.getWaitQueue().addLast(this);
        ready = false;
    }

    protected Condition newCondition() {
        return new Condition(dispatcher);
    }

    void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    boolean isReady() {
        return ready;
    }

    void setReady() {
        this.ready = true;
    }
}
