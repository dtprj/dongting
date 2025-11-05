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
package com.github.dtprj.dongting.raft;

/**
 * @author huangli
 */
public class RaftClientConfig {
    /**
     * This is an advanced option, usually you don't need to change it.
     * <p>
     * If false, the callback of async operations will be executed in NioWorker IO thread, it's single thread,
     * you should never perform any blocking or CPU-intensive operations within the callbacks.
     * If true, the callback will be executed in NioClient's bizExecutor, which is configurable use bizThreads
     * fields in NioClientConfig when constructing KvClient.
     */
    public boolean useBizExecutor = true;

    public long rpcTimeoutMillis = 5 * 1000L;
}
