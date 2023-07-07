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
package com.github.dtprj.dongting.raft;

import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.net.Peer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class GroupInfo {
    private final int groupId;
    private final List<Peer> servers;
    private volatile Pair<Peer, CompletableFuture<Peer>> leader;

    public GroupInfo(int groupId, List<Peer> servers) {
        this.groupId = groupId;
        this.servers = Collections.unmodifiableList(servers);
    }

    public int getGroupId() {
        return groupId;
    }

    public List<Peer> getServers() {
        return servers;
    }

    public Pair<Peer, CompletableFuture<Peer>> getLeader() {
        return leader;
    }

    public void setLeader(Pair<Peer, CompletableFuture<Peer>> leader) {
        this.leader = leader;
    }
}
