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

import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.Peer;

import java.util.Set;

/**
 * @author huangli
 */
class RaftMember {
    private final int id;
    private final Peer peer;
    private final Set<HostPort> servers;
    private final boolean self;

    public RaftMember(int id, Peer peer, Set<HostPort> servers, boolean self) {
        this.id = id;
        this.peer = peer;
        this.servers = servers;
        this.self = self;
    }

    public int getId() {
        return id;
    }

    public Peer getPeer() {
        return peer;
    }

    public Set<HostPort> getServers() {
        return servers;
    }

    public boolean isSelf() {
        return self;
    }
}
