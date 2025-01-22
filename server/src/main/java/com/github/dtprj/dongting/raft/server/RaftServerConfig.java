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

import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.buf.PoolFactory;

/**
 * @author huangli
 */
@SuppressWarnings("unused")
public class RaftServerConfig {
    private String servers;
    // internal use for raft log replication (server to server), and admin commands
    private int replicatePort;
    // use for client access, 0 indicates not start the client service server
    private int servicePort;
    private int nodeId;
    private long electTimeout = 15 * 1000;
    private long rpcTimeout = 5 * 1000;
    private long connectTimeout = 2000;
    private long heartbeatInterval = 2000;

    private boolean checkSelf = true;

    private int blockIoThreads = Math.max(Runtime.getRuntime().availableProcessors() * 2, 4);

    private PoolFactory poolFactory = new DefaultPoolFactory();

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public int getReplicatePort() {
        return replicatePort;
    }

    public void setReplicatePort(int replicatePort) {
        this.replicatePort = replicatePort;
    }

    public int getServicePort() {
        return servicePort;
    }

    public void setServicePort(int servicePort) {
        this.servicePort = servicePort;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public long getElectTimeout() {
        return electTimeout;
    }

    public void setElectTimeout(long electTimeout) {
        this.electTimeout = electTimeout;
    }

    public long getRpcTimeout() {
        return rpcTimeout;
    }

    public void setRpcTimeout(long rpcTimeout) {
        this.rpcTimeout = rpcTimeout;
    }

    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public boolean isCheckSelf() {
        return checkSelf;
    }

    public void setCheckSelf(boolean checkSelf) {
        this.checkSelf = checkSelf;
    }

    public PoolFactory getPoolFactory() {
        return poolFactory;
    }

    public void setPoolFactory(PoolFactory poolFactory) {
        this.poolFactory = poolFactory;
    }

    public int getBlockIoThreads() {
        return blockIoThreads;
    }

    public void setBlockIoThreads(int blockIoThreads) {
        this.blockIoThreads = blockIoThreads;
    }
}
