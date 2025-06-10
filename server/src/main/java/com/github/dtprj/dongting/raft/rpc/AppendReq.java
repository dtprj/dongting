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

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftRpcData;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Function;

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
//repeated LogItem entries = 8[packed=false];
//}
public class AppendReq extends RaftRpcData {
    private static final DtLog log = DtLogs.getLogger(AppendReq.class);

    public static final int IDX_GROUP_ID = 1;
    public static final int IDX_TERM = 2;
    public static final int IDX_LEADER_ID = 3;
    public static final int IDX_PREV_LOG_INDEX = 4;
    public static final int IDX_PREV_LOG_TERM = 5;
    public static final int IDX_LEADER_COMMIT = 6;
    public static final int IDX_LOGS_SIZE = 7;
    public static final int IDX_ENTRIES = 8;

    // private int groupId;
    // private int term;
    public int leaderId;
    public long prevLogIndex;
    public int prevLogTerm;
    public long leaderCommit;
    public ArrayList<LogItem> logs;

    // re-used
    public static class Callback extends PbCallback<AppendReq> {

        private final Function<Integer, RaftCodecFactory> decoderFactory;
        private AppendReq result;

        private final LogItemCallback logItemCallback = new LogItemCallback();

        public Callback(Function<Integer, RaftCodecFactory> decoderFactory) {
            this.decoderFactory = decoderFactory;
        }

        @Override
        protected void begin(int len) {
            result = new AppendReq();
        }

        @Override
        protected boolean end(boolean success) {
            if (!success) {
                RaftUtil.release(result.logs);
            }
            result = null;
            logItemCallback.codecFactory = null;
            return success;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
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
                    result.logs = new ArrayList<>((int) value);
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case IDX_PREV_LOG_INDEX:
                    result.prevLogIndex = value;
                    break;
                case IDX_LEADER_COMMIT:
                    result.leaderCommit = value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int len, int currentPos) {
            boolean end = buf.remaining() >= len - currentPos;
            if (index == IDX_ENTRIES) {
                if (logItemCallback.codecFactory == null) {
                    logItemCallback.codecFactory = decoderFactory.apply(result.groupId);
                    if (logItemCallback.codecFactory == null) {
                        log.error("can't find raft group codecFactory: {}", result.groupId);
                        // cancel parse, so return null, but parent parser not canceled,
                        // we will get a ReadPacket with null body
                        return false;
                    }
                }
                LogItem i = (LogItem) parseNested(buf, len, currentPos, logItemCallback);
                if (end) {
                    if (result.logs == null) {
                        result.logs = new ArrayList<>();
                    }
                    result.logs.add(i);
                }
            }
            return true;
        }

        @Override
        public AppendReq getResult() {
            return result;
        }
    }
}

