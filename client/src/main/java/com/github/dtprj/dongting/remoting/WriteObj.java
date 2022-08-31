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

import com.github.dtprj.dongting.common.DtTime;

import java.util.concurrent.CompletableFuture;

class WriteObj {
    private DtChannel dtc;

    private final Frame data;
    private final DtTime timeout;
    private final CompletableFuture<Frame> future;

    public WriteObj(Frame data, DtTime timeout, CompletableFuture<Frame> future) {
        this.data = data;
        this.timeout = timeout;
        this.future = future;
    }

    public DtChannel getDtc() {
        return dtc;
    }

    public void setDtc(DtChannel dtc) {
        this.dtc = dtc;
    }

    public Frame getData() {
        return data;
    }

    public DtTime getTimeout() {
        return timeout;
    }

    public CompletableFuture<Frame> getFuture() {
        return future;
    }
}
