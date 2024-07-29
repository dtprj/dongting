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

import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.common.DtTime;

/**
 * @author huangli
 */
class WriteData {
    private DtChannel dtc;

    private final WriteFrame data;
    private final DtTime timeout;

    int estimateSize;

    long perfTime;

    // only for request or one way request
    private final Peer peer;
    final RpcCallback<?> callback;
    private final Decoder<?> respDecoder;

    // for request or one way request
    public <T> WriteData(Peer peer, WriteFrame data, DtTime timeout, RpcCallback<T> callback, Decoder<T> respDecoder) {
        this.peer = peer;
        this.data = data;
        this.timeout = timeout;
        this.callback = callback;
        this.respDecoder = respDecoder;
    }

    // for response
    public WriteData(DtChannel dtc, WriteFrame data, DtTime timeout) {
        this.dtc = dtc;
        this.peer = null;
        this.data = data;
        this.timeout = timeout;
        this.callback = null;
        this.respDecoder = null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    void callSuccess(ReadFrame resp) {
        RpcCallback.callSuccess(callback, resp);
    }

    void callFail(boolean callClean, Throwable ex) {
        if (callClean) {
            data.clean();
        }
        RpcCallback.callFail(callback, ex);
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

    public Decoder<?> getRespDecoder() {
        return respDecoder;
    }

    public Peer getPeer() {
        return peer;
    }

    public void setDtc(DtChannel dtc) {
        this.dtc = dtc;
    }
}
