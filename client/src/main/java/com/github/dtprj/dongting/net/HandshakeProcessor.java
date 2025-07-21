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
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

/**
 * @author huangli
 */
class HandshakeProcessor extends ReqProcessor<HandshakeBody> {
    private static final DtLog log = DtLogs.getLogger(HandshakeProcessor.class);

    protected final NioServerConfig config;

    public HandshakeProcessor(NioServerConfig config) {
        this.config = config;
    }

    @Override
    public DecoderCallback<HandshakeBody> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new HandshakeBody());
    }

    @Override
    public WritePacket process(ReadPacket<HandshakeBody> packet, ReqContext reqContext) throws Exception {
        DtChannelImpl dtc = (DtChannelImpl) reqContext.getDtChannel();
        if (dtc.handshake) {
            log.warn("handshake already done: {}", dtc.getChannel());
            return null;
        }

        HandshakeBody hb = new HandshakeBody();
        hb.majorVersion = DtUtil.RPC_MAJOR_VER;
        hb.minorVersion = DtUtil.RPC_MINOR_VER;

        if (hb.processInfo != null) {
            dtc.remoteUuid1 = hb.processInfo.uuid1;
            dtc.remoteUuid2 = hb.processInfo.uuid2;
        }

        if (config.serverHint) {
            hb.config = buildServerHint();
        }

        SimpleWritePacket p = new SimpleWritePacket(hb);
        p.setRespCode(CmdCodes.SUCCESS);

        dtc.workerStatus.worker.finishHandshake(dtc);

        return p;
    }

    protected ConfigBody buildServerHint() {
        ConfigBody cb = new ConfigBody();
        cb.maxPacketSize = config.maxPacketSize;
        cb.maxBodySize = config.maxBodySize;
        cb.maxOutPending = config.maxInRequests >>> 3;
        cb.maxOutPendingBytes = config.maxInBytes >>> 3;
        return cb;
    }

}
