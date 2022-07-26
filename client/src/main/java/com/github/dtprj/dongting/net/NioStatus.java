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

import com.github.dtprj.dongting.common.IntObjMap;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author huangli
 */
class NioStatus {
    private final IntObjMap<ReqProcessor> processors = new IntObjMap<>();
    private final AtomicLong inReqBytes = new AtomicLong();
    private Semaphore requestSemaphore;

    public ReqProcessor getProcessor(int cmd) {
        return processors.get(cmd);
    }

    public void registerProcessor(int cmd, ReqProcessor processor) {
        processors.put(cmd, processor);
    }

    public IntObjMap<ReqProcessor> getProcessors() {
        return processors;
    }

    public AtomicLong getInReqBytes() {
        return inReqBytes;
    }

    public Semaphore getRequestSemaphore() {
        return requestSemaphore;
    }

    public void setRequestSemaphore(Semaphore requestSemaphore) {
        this.requestSemaphore = requestSemaphore;
    }

}
