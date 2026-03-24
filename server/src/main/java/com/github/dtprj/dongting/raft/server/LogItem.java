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
public class LogItem {
    //message LogItem {
    //    int32 type = 1;
    //    int32 bizType = 2;
    //    int32 term = 3;
    //    fixed64 index = 4;
    //    int32 prev_log_term = 5;
    //    fixed64 timestamp = 6;
    //    bytes header = 7;
    //    bytes body = 8;
    //}
    public static final int IDX_TYPE = 1;
    public static final int IDX_BIZ_TYPE = 2;
    public static final int IDX_TERM = 3;
    public static final int IDX_INDEX = 4;
    public static final int IDX_PREV_LOG_TERM = 5;
    public static final int IDX_TIMESTAMP = 6;
    public static final int IDX_HEADER = 7;
    public static final int IDX_BODY = 8;

    public static final int TYPE_NORMAL = 0;
    public static final int TYPE_HEARTBEAT = 1;
    public static final int TYPE_PREPARE_CONFIG_CHANGE = 2;
    public static final int TYPE_DROP_CONFIG_CHANGE = 3;
    public static final int TYPE_COMMIT_CONFIG_CHANGE = 4;
    public static final int TYPE_LOG_READ = 5;

    public int type;
    public int bizType;
    public int term;
    public long index;
    public int prevLogTerm;
    public long timestamp;

    public RaftReqData reqData;

    public int pbHeaderSize;
    public int pbItemSize;

    public LogItem() {
    }

}
