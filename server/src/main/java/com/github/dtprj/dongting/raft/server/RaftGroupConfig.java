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
    private int groupId;
    private String nodeIdOfMembers;
    private String nodeIdOfLearners;
    private String dataDir = "./data";
    private String statusFile = "raft.status";

    public String getNodeIdOfMembers() {
        return nodeIdOfMembers;
    }

    public void setNodeIdOfMembers(String nodeIdOfMembers) {
        this.nodeIdOfMembers = nodeIdOfMembers;
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

    public int getGroupId() {
        return groupId;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    public String getNodeIdOfLearners() {
        return nodeIdOfLearners;
    }

    public void setNodeIdOfLearners(String nodeIdOfLearners) {
        this.nodeIdOfLearners = nodeIdOfLearners;
    }
}
