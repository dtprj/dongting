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
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class VoteTest extends ServerTestBase {

    @Test
    void test() throws Exception {
        String servers = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003";
        String members = "1,2,3";
        ServerInfo s1 = createServer(1, servers, members);
        ServerInfo s2 = createServer(2, servers, members);
        ServerInfo s3 = createServer(3, servers, members);

        s1.raftServer.getAllMemberReadyFuture().get();
        s2.raftServer.getAllMemberReadyFuture().get();
        s3.raftServer.getAllMemberReadyFuture().get();

        TestUtil.waitUtil(() -> {
            int leader = 0;
            if (s1.raftServer.getRaftGroup(1).isLeader()) {
                leader++;
            }
            if (s2.raftServer.getRaftGroup(1).isLeader()) {
                leader++;
            }
            if (s3.raftServer.getRaftGroup(1).isLeader()) {
                leader++;
            }
            return leader == 1;
        });

        DtTime timeout = new DtTime(5, TimeUnit.SECONDS);
        s1.raftServer.stop(timeout);
        s2.raftServer.stop(timeout);
        s3.raftServer.stop(timeout);
    }
}
