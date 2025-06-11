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
package com.github.dtprj.dongting.raft;

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.codec.SimpleEncodable;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;

/**
 * @author huangli
 */
public class QueryStatusResp extends RaftConfigRpcData implements SimpleEncodable {
//    uint32 group_id = 1;
//    uint32 node_id = 2;
//    uint32 term = 3;
//    uint32 leader_id = 4;
//    fixed64 commit_index = 5;
//    fixed64 last_applied = 6;
//    fixed64 last_apply_to_now_millis = 7;
//    fixed64 last_log_index = 8;
//    fixed64 apply_lag_millis = 9;
//    repeated fixed32 members = 10[packed = false];
//    repeated fixed32 observers = 11[packed = false];
//    repeated fixed32 prepared_members = 12[packed = false];
//    repeated fixed32 prepared_observers = 13[packed = false];

    private static final int IDX_GROUP_ID = 1;
    private static final int IDX_NODE_ID = 2;
    private static final int IDX_TERM = 3;
    private static final int IDX_LEADER_ID = 4;
    private static final int IDX_COMMIT_INDEX = 5;
    private static final int IDX_LAST_APPLIED = 6;
    private static final int IDX_LAST_APPLY_TIME_TO_NOW_MILLIS = 7;
    private static final int IDX_LAST_LOG_INDEX = 8;
    private static final int IDX_APPLY_LAG_MILLIS = 9;
    private static final int IDX_MEMBERS = 10;
    private static final int IDX_OBSERVERS = 11;
    private static final int IDX_PREPARED_MEMBERS = 12;
    private static final int IDX_PREPARED_OBSERVERS = 13;

    public int nodeId;
    public int leaderId;
    public long commitIndex;
    public long lastApplied;
    public long lastApplyTimeToNowMillis;
    public long lastLogIndex;
    public long applyLagMillis; // the time delay from commit to apply, sampled update.

    public static final DecoderCallbackCreator<QueryStatusResp> DECODER = ctx -> ctx.toDecoderCallback(
            new Callback());

    private int size;

    public QueryStatusResp() {
    }

    @Override
    public int actualSize() {
        if(size == 0) {
            size = PbUtil.sizeOfInt32Field(IDX_GROUP_ID, groupId) +
                    PbUtil.sizeOfInt32Field(IDX_NODE_ID, nodeId) +
                    PbUtil.sizeOfInt32Field(IDX_TERM, term) +
                    PbUtil.sizeOfInt32Field(IDX_LEADER_ID, leaderId) +
                    PbUtil.sizeOfFix64Field(IDX_COMMIT_INDEX, commitIndex) +
                    PbUtil.sizeOfFix64Field(IDX_LAST_APPLIED, lastApplied) +
                    PbUtil.sizeOfFix64Field(IDX_LAST_APPLY_TIME_TO_NOW_MILLIS, lastApplyTimeToNowMillis) +
                    PbUtil.sizeOfFix64Field(IDX_LAST_LOG_INDEX, lastLogIndex) +
                    PbUtil.sizeOfFix64Field(IDX_APPLY_LAG_MILLIS, applyLagMillis) +
                    PbUtil.sizeOfFix32Field(IDX_MEMBERS, members) +
                    PbUtil.sizeOfFix32Field(IDX_OBSERVERS, observers) +
                    PbUtil.sizeOfFix32Field(IDX_PREPARED_MEMBERS, preparedMembers) +
                    PbUtil.sizeOfFix32Field(IDX_PREPARED_OBSERVERS, preparedObservers);
        }
        return size;
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeInt32Field(buf, IDX_GROUP_ID, groupId);
        PbUtil.writeInt32Field(buf, IDX_NODE_ID, nodeId);
        PbUtil.writeInt32Field(buf, IDX_TERM, term);
        PbUtil.writeInt32Field(buf, IDX_LEADER_ID, leaderId);
        PbUtil.writeFix64Field(buf, IDX_COMMIT_INDEX, commitIndex);
        PbUtil.writeFix64Field(buf, IDX_LAST_APPLIED, lastApplied);
        PbUtil.writeFix64Field(buf, IDX_LAST_APPLY_TIME_TO_NOW_MILLIS, lastApplyTimeToNowMillis);
        PbUtil.writeFix64Field(buf, IDX_LAST_LOG_INDEX, lastLogIndex);
        PbUtil.writeFix64Field(buf, IDX_APPLY_LAG_MILLIS, applyLagMillis);
        PbUtil.writeFix32Field(buf, IDX_MEMBERS, members);
        PbUtil.writeFix32Field(buf, IDX_OBSERVERS, observers);
        PbUtil.writeFix32Field(buf, IDX_PREPARED_MEMBERS, preparedMembers);
        PbUtil.writeFix32Field(buf, IDX_PREPARED_OBSERVERS, preparedObservers);
    }

    public static final class Callback extends PbCallback<QueryStatusResp> {
        private final QueryStatusResp resp = new QueryStatusResp();

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case IDX_GROUP_ID:
                    resp.groupId = (int) value;
                    break;
                case IDX_NODE_ID:
                    resp.nodeId = (int) value;
                    break;
                case IDX_TERM:
                    resp.term = (int) value;
                    break;
                case IDX_LEADER_ID:
                    resp.leaderId = (int) value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix32(int index, int value) {
            switch (index) {
                case IDX_MEMBERS:
                    if (resp.members == Collections.EMPTY_SET) {
                        resp.members = new HashSet<>();
                    }
                    resp.members.add(value);
                    break;
                case IDX_OBSERVERS:
                    if (resp.observers == Collections.EMPTY_SET) {
                        resp.observers = new HashSet<>();
                    }
                    resp.observers.add(value);
                    break;
                case IDX_PREPARED_MEMBERS:
                    if (resp.preparedMembers == Collections.EMPTY_SET) {
                        resp.preparedMembers = new HashSet<>();
                    }
                    resp.preparedMembers.add(value);
                    break;
                case IDX_PREPARED_OBSERVERS:
                    if (resp.preparedObservers == Collections.EMPTY_SET) {
                        resp.preparedObservers = new HashSet<>();
                    }
                    resp.preparedObservers.add(value);
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case IDX_COMMIT_INDEX:
                    resp.commitIndex = value;
                    break;
                case IDX_LAST_APPLIED:
                    resp.lastApplied = value;
                    break;
                case IDX_LAST_APPLY_TIME_TO_NOW_MILLIS:
                    resp.lastApplyTimeToNowMillis = value;
                    break;
                case IDX_LAST_LOG_INDEX:
                    resp.lastLogIndex = value;
                    break;
                case IDX_APPLY_LAG_MILLIS:
                    resp.applyLagMillis = value;
                    break;
            }
            return true;
        }

        @Override
        public QueryStatusResp getResult() {
            return resp;
        }
    }
}
