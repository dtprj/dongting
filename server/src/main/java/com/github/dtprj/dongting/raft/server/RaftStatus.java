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
public abstract class RaftStatus {
    // in raft paper: persistent state on all servers
    public int currentTerm;
    public int votedFor;

    // in raft paper: volatile state on all servers
    public long commitIndex;

    public final int groupId;

    protected RaftStatus(int groupId) {
        this.groupId = groupId;
    }

}
