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
    private String servers;
    private int raftPort;
    private int bizPort;
    private int id;
    private long electTimeout = 15 * 1000;
    private long rpcTimeout = 5 * 1000;
    private long heartbeatInterval = 2000;
    private int maxBodySize = 4 * 1024 * 1024;
    private int maxReplicateItems = 1000;
    private long maxReplicateBytes = 16 * 1024 * 1024;

    private int maxPendingWrites = 10000;
    private long maxPendingWriteBytes = 256 * 1024 * 1024;

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public int getRaftPort() {
        return raftPort;
    }

    public void setRaftPort(int raftPort) {
        this.raftPort = raftPort;
    }

    public int getBizPort() {
        return bizPort;
    }

    public void setBizPort(int bizPort) {
        this.bizPort = bizPort;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public int getMaxReplicateItems() {
        return maxReplicateItems;
    }

    public void setMaxReplicateItems(int maxReplicateItems) {
        this.maxReplicateItems = maxReplicateItems;
    }

    public long getMaxReplicateBytes() {
        return maxReplicateBytes;
    }

    public void setMaxReplicateBytes(long maxReplicateBytes) {
        this.maxReplicateBytes = maxReplicateBytes;
    }

    public int getMaxBodySize() {
        return maxBodySize;
    }

    public void setMaxBodySize(int maxBodySize) {
        this.maxBodySize = maxBodySize;
    }

    public int getMaxPendingWrites() {
        return maxPendingWrites;
    }

    public void setMaxPendingWrites(int maxPendingWrites) {
        this.maxPendingWrites = maxPendingWrites;
    }

    public long getMaxPendingWriteBytes() {
        return maxPendingWriteBytes;
    }

    public void setMaxPendingWriteBytes(long maxPendingWriteBytes) {
        this.maxPendingWriteBytes = maxPendingWriteBytes;
    }
}
