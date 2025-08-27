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
package com.github.dtprj.dongting.dtkv;

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WritePacket;

/**
 * @author huangli
 */
class WatchProcessor extends ReqProcessor<WatchNotifyReq> {

    private final WatchManager manager;

    WatchProcessor(WatchManager manager) {
        this.manager = manager;
    }

    @Override
    public DecoderCallback<WatchNotifyReq> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new WatchNotifyReq.Callback());
    }

    @Override
    public WritePacket process(ReadPacket<WatchNotifyReq> packet, ReqContext reqContext) throws Exception {
        WatchNotifyReq req = packet.getBody();
        if (req != null) {
            return manager.processNotify(req, reqContext.getDtChannel().getRemoteAddr());
        } else {
            return new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR);
        }
    }
}
