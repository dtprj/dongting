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
public interface CmdCodes {
    int SUCCESS = 0;
    int CLIENT_ERROR = 1;
    int SYS_ERROR = 2;
    int COMMAND_NOT_SUPPORT = 3;
    int STOPPING = 4;
    int BIZ_ERROR = 5;
    int FLOW_CONTROL = 6;
    int NOT_RAFT_LEADER = 7;
    int RAFT_GROUP_NOT_FOUND = 8;
    int RAFT_GROUP_STOPPED = 9;
    int RAFT_GROUP_NOT_INIT = 10;
}
