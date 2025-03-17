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
package com.github.dtprj.dongting.raft.impl;

import static com.github.dtprj.dongting.util.Tick.tick;

/**
 * @author huangli
 */
public class ImplAccessor {
    public static void updateNodeManager(NodeManager nodeManager) {
        nodeManager.pingIntervalMillis = 1;
    }

    public static void updateMemberManager(MemberManager memberManager) {
        memberManager.daemonSleepInterval = 1;
    }

    public static void updateVoteManager(VoteManager voteManager) {
        voteManager.firstDelayMin = 0;
        voteManager.firstDelayMax = tick(5);
        voteManager.checkIntervalMin = 1;
        voteManager.checkIntervalMax = tick(5);
    }
}
