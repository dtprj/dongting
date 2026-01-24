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
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.PacketType;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WritePacket;

/**
 * @author huangli
 */
class KvClientProcessor extends ReqProcessor<Object> {

    private final WatchManager watchManager;
    private final LockManager lockManager;

    KvClientProcessor(WatchManager watchManager, LockManager lockManager) {
        this.watchManager = watchManager;
        this.lockManager = lockManager;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public DecoderCallback createDecoderCallback(int command, DecodeContext context) {
        if (command == Commands.DTKV_WATCH_NOTIFY_PUSH) {
            return context.toDecoderCallback(new WatchNotifyReq.Callback());
        } else {
            // lock push
            return context.toDecoderCallback(context.kvReqCallback());
        }
    }

    @Override
    public WritePacket process(ReadPacket<Object> packet, ReqContext reqContext) throws Exception {
        Object body = packet.getBody();
        if (body == null) {
            return new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR);
        }
        if (packet.command == Commands.DTKV_WATCH_NOTIFY_PUSH) {
            WatchNotifyReq req = (WatchNotifyReq) body;
            return watchManager.processNotify(req, reqContext.getDtChannel().getRemoteAddr());
        } else if (packet.command == Commands.DTKV_LOCK_PUSH) {
            KvReq req = (KvReq) body;
            lockManager.processLockPush(req.groupId, req, packet.bizCode);
            if (packet.packetType == PacketType.TYPE_ONE_WAY) {
                return null;
            } else {
                // currently, lock push is always one-way,
                // but we keep the code in case it changes in the future
                return new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            }
        } else {
            return new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR);
        }
    }
}
