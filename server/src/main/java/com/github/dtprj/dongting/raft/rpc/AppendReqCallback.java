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

import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.pb.PbParser;
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
public class AppendReqCallback extends PbCallback {

    private int groupId;
    private int term;
    private int leaderId;
    private long prevLogIndex;
    private int prevLogTerm;
    private final ArrayList<LogItem> logs = new ArrayList<>();
    private long leaderCommit;

    public AppendReqCallback() {
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
            PbParser logItemParser;
            if (begin) {
                logItemParser = parser.createOrGetNestedParserSingle(new LogItemCallback(), len);
            } else {
                logItemParser = parser.getNestedParser();
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
    //  bytes data = 5;
    //}
    static class LogItemCallback extends PbCallback {
        private final LogItem item = new LogItem();

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
            if (index == 3) {
                item.setIndex(value);
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int len, boolean begin, boolean end) {
            if (index == 5) {
                ByteBuffer dest;
                if (begin) {
                    dest = ByteBuffer.allocate(len);
                    item.setBuffer(dest);
                } else {
                    dest = item.getBuffer();
                }
                dest.put(buf);
                if (end) {
                    dest.flip();
                }
            }
            return true;
        }
    }
}
