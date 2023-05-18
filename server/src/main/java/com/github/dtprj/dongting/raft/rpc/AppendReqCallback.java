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
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.codec.PbParser;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftGroups;
import com.github.dtprj.dongting.raft.server.LogItem;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author huangli
 */
//message AppendEntriesReq {
//  uint32 group_id = 1;
//  uint32 term = 2;
//  uint32 leader_id = 3;
//  fixed64 prev_log_index = 4;
//  uint32 prev_log_term = 5;
//  repeated LogItem entries = 6;
//  fixed64 leader_commit = 7;
//}
//
public class AppendReqCallback extends PbCallback<AppendReqCallback> {

    private final DecodeContext context;
    private final RaftGroups raftGroups;
    private int groupId;
    private int term;
    private int leaderId;
    private long prevLogIndex;
    private int prevLogTerm;
    private final ArrayList<LogItem> logs = new ArrayList<>();
    private long leaderCommit;

    public AppendReqCallback(DecodeContext context, RaftGroups raftGroups) {
        this.context = context;
        this.raftGroups = raftGroups;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        switch (index) {
            case 1:
                groupId = (int) value;
                break;
            case 2:
                term = (int) value;
                break;
            case 3:
                leaderId = (int) value;
                break;
            case 5:
                prevLogTerm = (int) value;
                break;
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        switch (index) {
            case 4:
                prevLogIndex = value;
                break;
            case 7:
                leaderCommit = value;
                break;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int len, boolean begin, boolean end) {
        if (index == 6) {
            RaftGroupImpl group = raftGroups.get(groupId);
            if (group == null) {
                // group id should encode before entries
                throw new PbException("can't find raft group: " + groupId);
            }
            PbParser logItemParser;
            DecodeContext nestedContext = context.createOrGetNestedContext(begin);
            if (begin) {
                Decoder decoder = (Decoder) group.getStateMachine().getDecoder().get();
                LogItemCallback c = new LogItemCallback(nestedContext, decoder);
                logItemParser = nestedContext.createOrResetPbParser(c, len);
            } else {
                logItemParser = nestedContext.getPbParser();
            }
            LogItemCallback callback = (LogItemCallback) logItemParser.getCallback();
            logItemParser.parse(buf);
            if (end) {
                LogItem i = callback.item;
                logs.add(i);
            }
        }
        return true;
    }

    @Override
    public AppendReqCallback getResult() {
        return this;
    }


    public int getGroupId() {
        return groupId;
    }

    public int getTerm() {
        return term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public ArrayList<LogItem> getLogs() {
        return logs;
    }

    //message LogItem {
    //  uint32 type = 1;
    //  uint32 term = 2;
    //  fixed64 index = 3;
    //  uint32 prev_log_term = 4;
    //  fixed64 timestamp = 5;
    //  bytes data = 6;
    //}
    static class LogItemCallback extends PbCallback<Object> {
        private final LogItem item = new LogItem();
        private final Decoder<?> stateMachineDecoder;
        private final DecodeContext context;

        public LogItemCallback(DecodeContext context, Decoder<?> stateMachineDecoder) {
            this.context = context;
            this.stateMachineDecoder = stateMachineDecoder;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    item.setType((int) value);
                    break;
                case 2:
                    item.setTerm((int) value);
                    break;
                case 4:
                    item.setPrevLogTerm((int) value);
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case 3:
                    item.setIndex(value);
                    break;
                case 5:
                    item.setTimestamp(value);
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int len, boolean begin, boolean end) {
            if (index == 6) {
                if (begin) {
                    item.setDataSize(len);
                }
                Object result;
                if (item.getType() == LogItem.TYPE_NORMAL) {
                    result = stateMachineDecoder.decode(context, buf, len, begin, end);
                } else {
                    result = Decoder.decodeToByteBuffer(buf, len, begin, end, (ByteBuffer) item.getData());
                }
                if (end) {
                    item.setData(result);
                }
            }
            return true;
        }
    }
}
