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

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.common.DtTime;

import java.util.function.Function;

/**
 * @author huangli
 */
class WriteData {
    private DtChannelImpl dtc;

    private final WritePacket data;
    private final DtTime timeout;

    final int estimateSize;

    long perfTime;

    // only for request or one way request
    private final Peer peer;
    final RpcCallback<?> callback;
    final Function<DecodeContext, DecoderCallback<?>> respDecoderCallback;

    // for request or one way request (client side)
    public <T> WriteData(Peer peer, WritePacket data, DtTime timeout, RpcCallback<T> callback,
                         Function<DecodeContext, DecoderCallback<T>> respDecoderCallback) {
        this.peer = peer;
        this.data = data;
        this.timeout = timeout;
        this.callback = callback;
        Function t = respDecoderCallback;
        this.respDecoderCallback = t;
        this.estimateSize = data.calcMaxPacketSize();
    }

    // for request or one way request (server push)
    public <T> WriteData(DtChannelImpl dtc, WritePacket data, DtTime timeout, RpcCallback<T> callback,
                         Function<DecodeContext, DecoderCallback<T>> respDecoderCallback) {
        this.dtc = dtc;
        this.peer = null;
        this.data = data;
        this.timeout = timeout;
        this.callback = callback;
        Function t = respDecoderCallback;
        this.respDecoderCallback = t;
        this.estimateSize = data.calcMaxPacketSize();
    }

    // for response
    public WriteData(DtChannelImpl dtc, WritePacket data, DtTime timeout) {
        this.dtc = dtc;
        this.peer = null;
        this.data = data;
        this.timeout = timeout;
        this.callback = null;
        this.respDecoderCallback = null;
        this.estimateSize = data.calcMaxPacketSize();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    void callSuccess(ReadPacket resp) {
        if (data.packetType == PacketType.TYPE_REQ && resp != null && resp.respCode != CmdCodes.SUCCESS) {
            RpcCallback.callFail(callback, new NetCodeException(resp.respCode, resp.msg, resp));
        } else {
            RpcCallback.callSuccess(callback, resp);
        }
    }

    void callFail(boolean callClean, Throwable ex) {
        if (callClean) {
            data.clean();
        }
        RpcCallback.callFail(callback, ex);
    }

    public DtChannelImpl getDtc() {
        return dtc;
    }

    public WritePacket getData() {
        return data;
    }

    public DtTime getTimeout() {
        return timeout;
    }

    public Peer getPeer() {
        return peer;
    }

    public void setDtc(DtChannelImpl dtc) {
        this.dtc = dtc;
    }
}
