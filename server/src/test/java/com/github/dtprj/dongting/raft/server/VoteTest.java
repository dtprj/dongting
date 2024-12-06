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

import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class VoteTest extends ServerTestBase {

    private int waitLeaderElectAndGetLeaderId(ServerInfo... servers) {
        AtomicInteger leaderId = new AtomicInteger();
        TestUtil.waitUtil(() -> {
            int leader = 0;
            for (ServerInfo server : servers) {
                if (server.raftServer.getRaftGroup(1).isLeader()) {
                    leader++;
                    leaderId.set(server.nodeId);
                }
            }
            return leader == 1;
        });
        return leaderId.get();
    }

    @Test
    void test() throws Exception {
        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003;4,127.0.0.1:4004";
        String members = "1,2,3";
        String observers = "4";
        ServerInfo[] sis = new ServerInfo[4];
        sis[0] = createServer(1, servers, members, observers);
        sis[1] = createServer(2, servers, members, observers);
        sis[2] = createServer(3, servers, members, observers);
        sis[3] = createServer(4, servers, members, observers);

        for (ServerInfo si : sis) {
            si.raftServer.getAllMemberReadyFuture().get();
        }

        int leader = waitLeaderElectAndGetLeaderId(sis);
        assertTrue(leader != 4);
        // DtTime timeout = new DtTime(5, TimeUnit.SECONDS);
        // sis[leader-1].raftServer.stop(timeout);
    }
}
