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
    // 1 ~ 15 for most common commands, in protobuf var int 1~15 use 1 byte
    int CMD_PING = 1;
    int CMD_HANDSHAKE = 2;
    int CMD_HEARTBEAT = 3;
    int RAFT_APPEND_ENTRIES = 4;
    int DTKV_GET = 5;
    int DTKV_PUT = 6;

    // 16 ~ 29 for rpc

    // 100 ~ 109 for raft server
    int NODE_PING = 100;
    int RAFT_PING = 101;
    int RAFT_REQUEST_VOTE = 102;
    int RAFT_INSTALL_SNAPSHOT = 103;
    int RAFT_LEADER_TRANSFER = 104;
    int RAFT_QUERY_STATUS = 105;

    // 110 ~ 119 for raft client

    // 120 ~ 139 for dt kv
    int DTKV_REMOVE = 122;
    int DTKV_MKDIR = 123;
}
