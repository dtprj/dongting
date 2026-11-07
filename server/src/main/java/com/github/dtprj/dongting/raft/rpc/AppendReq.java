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

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.ParseUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.common.DtCleanable;
import com.github.dtprj.dongting.raft.RaftRpcData;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.raft.store.RaftLogDataCallback;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author huangli
 */
//message AppendEntriesReq {
//int32 group_id = 1;
//int32 term = 2;
//int32 leader_id = 3;
//fixed64 prev_log_index = 4;
//int32 prev_log_term = 5;
//fixed64 leader_commit = 6;
//int32 logs_size = 7;
//fixed32 raft_cluster_id = 8;
//bytes entries = 15;
//}
public class AppendReq extends RaftRpcData implements DtCleanable {

    public static final int IDX_GROUP_ID = 1;
    public static final int IDX_TERM = 2;
    public static final int IDX_LEADER_ID = 3;
    public static final int IDX_PREV_LOG_INDEX = 4;
    public static final int IDX_PREV_LOG_TERM = 5;
    public static final int IDX_LEADER_COMMIT = 6;
    public static final int IDX_LOGS_SIZE = 7;
    public static final int IDX_RAFT_CLUSTER_ID = 8;
    public static final int IDX_ENTRIES = 15;

    // private int groupId;
    // private int term;
    public int leaderId;
    public long prevLogIndex;
    public int prevLogTerm;
    public long leaderCommit;
    public ArrayList<RaftTask> logs;
    public int raftClusterId;

    @Override
    public void clean() {
        if (logs != null) {
            RaftUtil.releaseInputs(logs);
            logs = null;
        }
    }

    // re-used
    public static class Callback implements PbCallback<AppendReq> {

        // biz header/body are decoded in AppendProcessor.preProcess()
        private final RaftLogDataCallback logDataCallback;

        private AppendReq result;

        public Callback() {
            this.logDataCallback = new RaftLogDataCallback(logData -> {
                if (result.logs == null) {
                    result.logs = new ArrayList<>();
                }
                result.logs.add(new RaftTask(logData, null, null,
                        logData.type == LogHeader.TYPE_LOG_READ));
            });
        }

        @Override
        public void begin(int len) {
            result = new AppendReq();
        }

        @Override
        public void end(boolean success, Throwable ex) {
            if (!success) {
                result.clean();
            }
            result = null;
        }

        @Override
        public void readVarNumber(DecodeContext context, int index, long value) {
            switch (index) {
                case IDX_GROUP_ID:
                    result.groupId = (int) value;
                    break;
                case IDX_TERM:
                    result.term = (int) value;
                    break;
                case IDX_LEADER_ID:
                    result.leaderId = (int) value;
                    break;
                case IDX_PREV_LOG_TERM:
                    result.prevLogTerm = (int) value;
                    break;
                case IDX_LOGS_SIZE:
                    if (result.logs == null) {
                        result.logs = ParseUtil.createArrayList((int) value);
                    }
                    break;
            }
        }

        @Override
        public void readFix64(DecodeContext context, int index, long value) {
            switch (index) {
                case IDX_PREV_LOG_INDEX:
                    result.prevLogIndex = value;
                    break;
                case IDX_LEADER_COMMIT:
                    result.leaderCommit = value;
                    break;
            }
        }

        @Override
        public void readFix32(DecodeContext context, int index, int value) {
            if (index == IDX_RAFT_CLUSTER_ID) {
                result.raftClusterId = value;
            }
        }

        @Override
        public void readBytes(DecodeContext context, int index, ByteBuffer buf, int len, int currentPos) {
            if (index == IDX_ENTRIES) {
                if (result.logs == null) {
                    result.logs = ParseUtil.createArrayList(8);
                }
                ParseUtil.parseNested(context, buf, len, currentPos, logDataCallback);
            }
        }

        @Override
        public AppendReq getResult(DecodeContext context) {
            return result;
        }
    }
}
