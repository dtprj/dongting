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

import com.github.dtprj.dongting.net.HostPort;

/**
 * @author huangli
 */
public class RaftNode {
    private final int nodeId;
    private final HostPort hostPort;

    private String str;

    public RaftNode(int nodeId, HostPort hostPort) {
        this.nodeId = nodeId;
        this.hostPort = hostPort;
    }

    public int getNodeId() {
        return nodeId;
    }

    public HostPort getHostPort() {
        return hostPort;
    }

    @Override
    public String toString() {
        if (str == null) {
            str = nodeId + "@" + hostPort;
        }
        return str;
    }
}
