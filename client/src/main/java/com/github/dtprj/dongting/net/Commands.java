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
package com.github.dtprj.dongting.net;

/**
 * @author huangli
 */
public interface Commands {
    // 1 ~ 15 for most common commands, in protobuf var int 1~15 use 1 byte, 16~2047 use 2 bytes
    int CMD_PING = 1;
    int CMD_HANDSHAKE = 2;
    int CMD_HEARTBEAT = 3;
    int RAFT_APPEND_ENTRIES = 4;
    int DTKV_GET = 5;
    int DTKV_PUT = 6;

    // 16 ~ 39 for rpc

    // 40 ~ 69 for raft
    int NODE_PING = 40;
    int RAFT_PING = 41;
    int RAFT_REQUEST_VOTE = 42;
    int RAFT_INSTALL_SNAPSHOT = 43;
    int RAFT_ADMIN_TRANSFER_LEADER = 44; // from admin tool to old leader
    int RAFT_TRANSFER_LEADER = 45; // from old leader to new leader
    int RAFT_QUERY_STATUS = 46;
    int RAFT_ADMIN_PREPARE_CHANGE = 47;
    int RAFT_ADMIN_COMMIT_CHANGE = 48;
    int RAFT_ADMIN_ABORT_CHANGE = 49;
    int RAFT_ADMIN_ADD_NODE = 50;
    int RAFT_ADMIN_REMOVE_NODE = 51;
    int RAFT_ADMIN_ADD_GROUP = 52;
    int RAFT_ADMIN_REMOVE_GROUP = 53;

    // 80 ~ 109 for dt kv
    int DTKV_REMOVE = 80;
    int DTKV_MKDIR = 81;
    int DTKV_LIST = 82;
    int DTKV_BATCH_GET = 83;
    int DTKV_BATCH_PUT = 84;
    int DTKV_BATCH_REMOVE = 85;
    int DTKV_CAS = 86;
    int DTKV_SYNC_WATCH = 87;
    int DTKV_WATCH_NOTIFY_PUSH = 88;
    int DTKV_QUERY_STATUS = 89;
    int DTKV_UPDATE_TTL = 90;
}
