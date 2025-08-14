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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;

/**
 * @author huangli
 */
public class KvServerUtil {

    /**
     * call after RaftServer init, before RaftServer start
     */
    public static void initKvServer(RaftServer server) {
        NioServer nioServer = server.getServiceNioServer();
        KvProcessor p = new KvProcessor(server);
        nioServer.register(Commands.DTKV_GET, p);
        nioServer.register(Commands.DTKV_PUT, p);
        nioServer.register(Commands.DTKV_REMOVE, p);
        nioServer.register(Commands.DTKV_MKDIR, p);
        nioServer.register(Commands.DTKV_LIST, p);
        nioServer.register(Commands.DTKV_BATCH_GET, p);
        nioServer.register(Commands.DTKV_BATCH_PUT, p);
        nioServer.register(Commands.DTKV_BATCH_REMOVE, p);
        nioServer.register(Commands.DTKV_CAS, p);
        nioServer.register(Commands.DTKV_PUT_TEMP_NODE, p);
        nioServer.register(Commands.DTKV_MAKE_TEMP_DIR, p);
        nioServer.register(Commands.DTKV_UPDATE_TTL, p);

        nioServer.register(Commands.DTKV_SYNC_WATCH, new WatchProcessor(server));

        nioServer.register(Commands.DTKV_QUERY_STATUS, new KvStatusProcessor(server));
    }

    static DtKV getStateMachine(ReqInfo<?> reqInfo) {
        StateMachine sm = reqInfo.raftGroup.getStateMachine();
        try {
            return (DtKV) sm;
        } catch (ClassCastException e) {
            EmptyBodyRespPacket errorResp = new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR);
            errorResp.setMsg("type error: " + sm);
            reqInfo.reqContext.writeRespInBizThreads(errorResp);
            return null;
        }
    }
}
