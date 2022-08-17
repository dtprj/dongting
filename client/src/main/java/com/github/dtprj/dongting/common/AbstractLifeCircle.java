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

public class AbstractLifeCircle implements LifeCircle {
    private boolean init;
    private boolean shutdown;

    @Override
    public final synchronized void start() throws Exception {
        if (!init) {
            doStart();
            init = true;
        } else {
            throw new IllegalStateException("already started");
        }
    }

    protected void doStart() throws Exception {
    }

    @Override
    public final synchronized void stop() throws Exception {
        if (init) {
            if (!shutdown) {
                doStop();
                shutdown = true;
            }
        } else {
            throw new IllegalStateException("not start");
        }
    }

    protected void doStop() throws Exception {
    }
}
