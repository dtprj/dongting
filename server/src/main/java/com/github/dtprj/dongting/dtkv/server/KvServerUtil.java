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

import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.raft.server.RaftServer;

/**
 * @author huangli
 */
public class KvServerUtil {

    /**
     * call after RaftServer init, before RaftServer start
     */
    public static void initKvServer(RaftServer server){
        NioServer nioServer = server.getServiceNioServer();
        KvProcessor p = new KvProcessor(server);
        nioServer.register(Commands.DTKV_GET, p);
        nioServer.register(Commands.DTKV_PUT, p);
        nioServer.register(Commands.DTKV_REMOVE, p);
        nioServer.register(Commands.DTKV_MKDIR, p);
        nioServer.register(Commands.DTKV_LIST, p);
    }
}
