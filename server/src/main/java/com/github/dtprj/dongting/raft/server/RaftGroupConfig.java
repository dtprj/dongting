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
public class RaftGroupConfig {
    private final int groupId;
    private final String nodeIdOfMembers;
    private final String nodeIdOfObservers;
    private String dataDir = "./data";
    private String statusFile = "raft.status";
    private long[] ioRetryInterval = new long[]{100, 1000, 3000, 5000, 10000, 20000};

    public RaftGroupConfig(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
        this.groupId = groupId;
        this.nodeIdOfMembers = nodeIdOfMembers;
        this.nodeIdOfObservers = nodeIdOfObservers;
    }

    public int getGroupId() {
        return groupId;
    }

    public String getNodeIdOfMembers() {
        return nodeIdOfMembers;
    }

    public String getNodeIdOfObservers() {
        return nodeIdOfObservers;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public String getStatusFile() {
        return statusFile;
    }

    public void setStatusFile(String statusFile) {
        this.statusFile = statusFile;
    }

    public long[] getIoRetryInterval() {
        return ioRetryInterval;
    }

    public void setIoRetryInterval(long[] ioRetryInterval) {
        this.ioRetryInterval = ioRetryInterval;
    }

}
