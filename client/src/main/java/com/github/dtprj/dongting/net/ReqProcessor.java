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
package com.github.dtprj.dongting.net;

import java.util.concurrent.ExecutorService;

/**
 * @author huangli
 */
public abstract class ReqProcessor {

    private ExecutorService executor;
    private boolean useDefaultExecutor;

    public abstract WriteFrame process(ReadFrame frame, DtChannel channel);

    public abstract Decoder getDecoder();

    ExecutorService getExecutor() {
        return executor;
    }

    void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    boolean isUseDefaultExecutor() {
        return useDefaultExecutor;
    }

    void setUseDefaultExecutor(boolean useDefaultExecutor) {
        this.useDefaultExecutor = useDefaultExecutor;
    }
}
