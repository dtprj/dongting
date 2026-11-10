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
package com.github.dtprj.dongting.dtmq.server;

import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;

public final class MqServerUtil {
    private MqServerUtil() {
    }

    public static void initMqServer(RaftServer server) {
        server.getNioServer().register(Commands.DTMQ_SEND, new MqSendProcessor(server), null);
        server.getNioServer().register(Commands.DTMQ_PULL, new MqPullProcessor(server), null);
    }

    static DtMQ getStateMachine(ReqInfo<?> reqInfo) {
        StateMachine sm = reqInfo.raftGroup.getStateMachine();
        if (sm instanceof DtMQ) {
            return (DtMQ) sm;
        }
        EmptyBodyRespPacket errorResp = new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR);
        errorResp.msg = "type error: " + sm;
        reqInfo.reqContext.writeRespInBizThreads(errorResp);
        return null;
    }
}
