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

import com.github.dtprj.dongting.common.RefCount;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.Peer;

/**
 * @author huangli
 */
class NodeInfo {
    private final int nodeId;
    private final HostPort hostPort;
    private final RefCount refCount = new RefCount();
    private final Peer peer;

    public NodeInfo(int nodeId, HostPort hostPort, Peer peer) {
        this.nodeId = nodeId;
        this.hostPort = hostPort;
        this.peer = peer;
    }

    public int getNodeId() {
        return nodeId;
    }

    public HostPort getHostPort() {
        return hostPort;
    }

    public RefCount getRefCount() {
        return refCount;
    }

    public Peer getPeer() {
        return peer;
    }
}

