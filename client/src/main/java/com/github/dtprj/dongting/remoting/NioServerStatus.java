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
package com.github.dtprj.dongting.remoting;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

class NioServerStatus {
    private final ConcurrentHashMap<Integer, CmdProcessor> processors = new ConcurrentHashMap<>();
    private ExecutorService bizExecutor;

    public void register(int cmd, CmdProcessor processor) {
        processors.put(cmd, processor);
    }

    public CmdProcessor getProcessor(int command) {
        return processors.get(command);
    }

    public ExecutorService getBizExecutor() {
        return bizExecutor;
    }

    public void setBizExecutor(ExecutorService bizExecutor) {
        this.bizExecutor = bizExecutor;
    }
}
