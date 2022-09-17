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

import com.github.dtprj.dongting.common.DtTime;

import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
class WriteData {
    private final DtChannel dtc;
    private final Peer peer;

    private final WriteFrame data;
    private final DtTime timeout;
    private final CompletableFuture<ReadFrame> future;
    private final Decoder decoder;

    public WriteData(Peer peer, WriteFrame data, DtTime timeout,
                     CompletableFuture<ReadFrame> future, Decoder decoder) {
        this.peer = peer;
        this.dtc = null;
        this.data = data;
        this.timeout = timeout;
        this.future = future;
        this.decoder = decoder;
    }

    public WriteData(DtChannel dtc, WriteFrame data) {
        this.dtc = dtc;
        this.peer = null;
        this.data = data;
        this.timeout = null;
        this.future = null;
        this.decoder = null;
    }

    public DtChannel getDtc() {
        return dtc;
    }

    public WriteFrame getData() {
        return data;
    }

    public DtTime getTimeout() {
        return timeout;
    }

    public CompletableFuture<ReadFrame> getFuture() {
        return future;
    }

    public Decoder getDecoder() {
        return decoder;
    }

    public Peer getEndPoint() {
        return peer;
    }
}
