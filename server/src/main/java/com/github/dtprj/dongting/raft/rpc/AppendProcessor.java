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
package com.github.dtprj.dongting.raft.rpc;

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

/**
 * @author huangli
 */
public class AppendProcessor {
    private static final DtLog log = DtLogs.getLogger(AppendProcessor.class);

    public static final int NO_RESULT_NOW = -1;
    public static final int CODE_SUCCESS = 0;
    public static final int CODE_LOG_NOT_MATCH = 1;
    public static final int CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT = 2;
    public static final int CODE_REQ_ERROR = 3;
    public static final int CODE_INSTALL_SNAPSHOT = 4;
    public static final int CODE_NOT_MEMBER_IN_GROUP = 5;
    public static final int CODE_SERVER_ERROR = 6;


    public static String getCodeStr(int code) {
        switch (code) {
            case CODE_SUCCESS:
                return "CODE_SUCCESS";
            case CODE_LOG_NOT_MATCH:
                return "CODE_LOG_NOT_MATCH";
            case CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT:
                return "CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT";
            case CODE_REQ_ERROR:
                return "CODE_REQ_ERROR";
            case CODE_INSTALL_SNAPSHOT:
                return "CODE_INSTALL_SNAPSHOT";
            case CODE_NOT_MEMBER_IN_GROUP:
                return "CODE_NOT_MEMBER_IN_GROUP";
            case CODE_SERVER_ERROR:
                return "CODE_SERVER_ERROR";
            default:
                return "CODE_UNKNOWN_" + code;
        }
    }
}
