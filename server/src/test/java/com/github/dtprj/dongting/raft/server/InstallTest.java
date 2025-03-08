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

import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.ShareStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class InstallTest extends ServerTestBase {

    @Test
    void test() throws Exception {
        ServerInfo s1 = createServer(1, "1,127.0.0.1:4001", "1", "");
        // index 1 is heart beat, term=1
        waitStart(s1);
        waitStop(s1);

        initTerm = 2;
        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003";
        String members = "1,2,3";
        String observers = "";

        ServerInfo s2 = createServer(2, servers, members, observers);
        ServerInfo s3 = createServer(3, servers, members, observers);
        waitStart(s2);
        waitStart(s3);

        initTerm = 0;
        s1 = createServer(1, servers, members, observers);
        waitStart(s1);
        RaftGroupImpl g = (RaftGroupImpl) s1.raftServer.getRaftGroup(1);
        ShareStatus ss = g.groupComponents.raftStatus.getShareStatus();
        assertEquals(RaftRole.follower, ss.role);
        assertTrue(ss.lastApplied >= 1);
        assertTrue(g.groupComponents.raftStatus.currentTerm > 2);

        waitStop(s1);
        waitStop(s2);
        waitStop(s3);
    }
}
