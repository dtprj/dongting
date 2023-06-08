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
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

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
//  fixed64 leader_commit = 6;
//  repeated LogItem entries = 7;
//}
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
            case 6:
                leaderCommit = value;
                break;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int len, int currentPos) {
        boolean end = buf.remaining() >= len - currentPos;
        if (index == 7) {
            RaftGroupImpl group = raftGroups.get(groupId);
            if (group == null) {
                // group id should encode before entries
                throw new PbException("can't find raft group: " + groupId);
            }
            PbParser logItemParser;
            DecodeContext nestedContext = context.createOrGetNestedContext(currentPos == 0);
            if (currentPos == 0) {
                LogItemCallback c = new LogItemCallback(nestedContext, group.getStateMachine());
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
    //  uint32 bizType = 2;
    //  uint32 term = 3;
    //  fixed64 index = 4;
    //  uint32 prev_log_term = 5;
    //  fixed64 timestamp = 6;
    //  bytes header = 7;
    //  bytes body = 8;
    //}
    static class LogItemCallback extends PbCallback<Object> {
        private final LogItem item = new LogItem();
        private final DecodeContext context;
        private final RaftCodecFactory codecFactory;

        public LogItemCallback(DecodeContext context, RaftCodecFactory codecFactory) {
            this.context = context;
            this.codecFactory = codecFactory;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    item.setType((int) value);
                    break;
                case 2:
                    item.setBizType((int) value);
                    break;
                case 3:
                    item.setTerm((int) value);
                    break;
                case 5:
                    item.setPrevLogTerm((int) value);
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case 4:
                    item.setIndex(value);
                    break;
                case 6:
                    item.setTimestamp(value);
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int len, int currentPos) {
            boolean begin = currentPos == 0;
            boolean end = buf.remaining() >= len - currentPos;
            if (index == 7) {
                if (begin) {
                    item.setActualHeaderSize(len);
                }
                Object result;
                if (item.getType() == LogItem.TYPE_NORMAL) {
                    Decoder<? extends Object> headerDecoder = codecFactory.createDecoder(item.getBizType(), true);
                    result = headerDecoder.decode(context, buf, len, currentPos);
                } else {
                    result = Decoder.decodeToByteBuffer(buf, len, currentPos, (ByteBuffer) item.getBody());
                }
                if (end) {
                    item.setActualHeaderSize(len);
                    item.setHeader(result);
                }
            } else if (index == 8) {
                if (begin) {
                    item.setActualBodySize(len);
                }
                Object result;
                if (item.getType() == LogItem.TYPE_NORMAL) {
                    Decoder<? extends Object> bodyDecoder = codecFactory.createDecoder(item.getBizType(), false);
                    result = bodyDecoder.decode(context, buf, len, currentPos);
                } else {
                    result = Decoder.decodeToByteBuffer(buf, len, currentPos, (ByteBuffer) item.getBody());
                }
                if (end) {
                    item.setActualBodySize(len);
                    item.setBody(result);
                }
            }
            return true;
        }
    }
}
