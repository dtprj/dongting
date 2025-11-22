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

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.FutureCallback;

/**
 * @author huangli
 */
final class PacketInfoReq extends PacketInfo {

    PacketInfoReq nextInChannel;
    PacketInfoReq prevInChannel;

    PacketInfoReq nearTimeoutQueueNext;
    PacketInfoReq nearTimeoutQueuePrev;

    final Peer peer;
    RpcCallback<?> callback;
    final DecoderCallbackCreator<?> respDecoderCallback;

    // for request or one way request (client side)
    public <T> PacketInfoReq(Peer peer, WritePacket packet, DtTime timeout, RpcCallback<T> callback,
                             DecoderCallbackCreator<T> respDecoderCallback) {
        super(null, packet, timeout);
        this.peer = peer;
        this.callback = callback;
        this.respDecoderCallback = respDecoderCallback;
    }

    // for request or one way request (server push), client handshake
    public <T> PacketInfoReq(DtChannelImpl dtc, WritePacket packet, DtTime timeout, RpcCallback<T> callback,
                             DecoderCallbackCreator<T> respDecoderCallback) {
        super(dtc, packet, timeout);
        this.peer = null;
        this.callback = callback;
        this.respDecoderCallback = respDecoderCallback;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    void callSuccess(ReadPacket resp) {
        if (callback == null) {
            return;
        }
        try {
            if (packet.packetType == PacketType.TYPE_REQ && resp != null && resp.respCode != CmdCodes.SUCCESS) {
                FutureCallback.callFail(callback, new NetCodeException(resp.respCode, resp.msg, resp.extra));
            } else {
                FutureCallback.callSuccess(callback, resp);
            }
        } finally {
            callback = null;
        }
    }

    void callFail(boolean callClean, Throwable ex) {
        if (callback == null) {
            return;
        }
        try {
            if (callClean) {
                packet.clean();
            }
            FutureCallback.callFail(callback, ex);
        } finally {
            callback = null;
        }
    }
}
