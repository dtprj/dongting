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

/**
 * @author huangli
 */
public class RaftServerConfig {
    public String servers;
    // internal use for raft log replication (server to server), and admin commands
    public int replicatePort;
    // use for client access, 0 indicates not start the client service server
    public int servicePort;
    public int nodeId;
    public long electTimeout = 15 * 1000;
    public long rpcTimeout = 5 * 1000;
    public long connectTimeout = 2000;
    public long heartbeatInterval = 2000;

    public boolean checkSelf = true;

    public int blockIoThreads = Math.max(Runtime.getRuntime().availableProcessors() * 2, 4);

}
