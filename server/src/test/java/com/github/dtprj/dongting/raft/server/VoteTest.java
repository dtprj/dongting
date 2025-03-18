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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.common.DtTime;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class VoteTest extends ServerTestBase {

    public VoteTest() {
        super(false);
    }

    @Test
    void testSimpleVote() throws Exception {
        // simple vote
        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003;4,127.0.0.1:4004";
        String members = "1,2,3";
        String observers = "4";
        ServerInfo[] sis = new ServerInfo[4];
        sis[0] = createServer(1, servers, members, observers);
        sis[1] = createServer(2, servers, members, observers);
        sis[2] = createServer(3, servers, members, observers);
        sis[3] = createServer(4, servers, members, observers);
        for (ServerInfo si : sis) {
            waitStart(si);
        }

        ServerInfo leader = waitLeaderElectAndGetLeaderId(groupId, sis);
        assertTrue(leader.nodeId != 4);

        DtTime timeout = new DtTime(5, TimeUnit.SECONDS);
        leader.raftServer.stop(timeout);

        ArrayList<ServerInfo> restServers = new ArrayList<>();
        for (ServerInfo si : sis) {
            if (si.nodeId != leader.nodeId) {
                restServers.add(si);
            }
        }

        ServerInfo newLeader = waitLeaderElectAndGetLeaderId(groupId, restServers.toArray(new ServerInfo[0]));
        assertTrue(newLeader.nodeId != leader.nodeId);

        for (ServerInfo si : restServers) {
            waitStop(si);
        }
    }
}
