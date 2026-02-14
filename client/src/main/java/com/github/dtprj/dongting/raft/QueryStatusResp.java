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

import com.github.dtprj.dongting.codec.CodecException;
import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author huangli
 */
public class QueryStatusResp extends RaftConfigRpcData implements Encodable {
//    int32 group_id = 1;
//    int32 node_id = 2;
//    int32 flag = 3;
//    int32 term = 4;
//    int32 leader_id = 5;
//    fixed64 commit_index = 6;
//    fixed64 last_applied = 7;
//    fixed64 last_apply_time_to_now_millis = 8;
//    fixed64 last_log_index = 9;
//    fixed64 apply_lag_millis = 10;
//    repeated fixed32 members = 11[packed = false];
//    repeated fixed32 observers = 12[packed = false];
//    repeated fixed32 prepared_members = 13[packed = false];
//    repeated fixed32 prepared_observers = 14[packed = false];
//    fixed64 last_config_change_index = 15;
//    string last_error = 16;

    private static final int IDX_GROUP_ID = 1;
    private static final int IDX_NODE_ID = 2;
    private static final int IDX_FLAG = 3;
    private static final int IDX_TERM = 4;
    private static final int IDX_LEADER_ID = 5;
    private static final int IDX_COMMIT_INDEX = 6;
    private static final int IDX_LAST_APPLIED = 7;
    private static final int IDX_LAST_APPLY_TIME_TO_NOW_MILLIS = 8;
    private static final int IDX_LAST_LOG_INDEX = 9;
    private static final int IDX_APPLY_LAG_MILLIS = 10;
    private static final int IDX_MEMBERS = 11;
    private static final int IDX_OBSERVERS = 12;
    private static final int IDX_PREPARED_MEMBERS = 13;
    private static final int IDX_PREPARED_OBSERVERS = 14;
    private static final int IDX_LAST_CONFIG_CHANGE_INDEX = 15;
    private static final int IDX_LAST_ERROR = 16;

    public int nodeId;
    private int flag;
    public int leaderId;
    public long commitIndex;
    public long lastApplied;
    public long lastApplyTimeToNowMillis;
    public long lastLogIndex;
    public long applyLagMillis; // the time delay from commit to apply, sampled update.
    public long lastConfigChangeIndex;
    public String lastError;

    public static final DecoderCallbackCreator<QueryStatusResp> DECODER = ctx -> ctx.toDecoderCallback(
            new Callback());

    private int size;

    private static final int FLAG_MASK_INIT_FINISHED = 1;
    private static final int FLAG_MASK_INIT_FAILED = 1 << 1;
    private static final int FLAG_MASK_GROUP_READY = 1 << 2;
    private static final int FLAG_BUG = 1 << 3;

    public QueryStatusResp() {
    }

    public void setFlag(boolean initFinished, boolean initFailed, boolean groupReady, boolean bug) {
        flag = 0;
        if (initFinished) {
            flag |= FLAG_MASK_INIT_FINISHED;
        }
        if (initFailed) {
            flag |= FLAG_MASK_INIT_FAILED;
        }
        if (groupReady) {
            flag |= FLAG_MASK_GROUP_READY;
        }
        if (bug) {
            flag |= FLAG_BUG;
        }
    }

    public boolean isInitFinished() {
        return (flag & FLAG_MASK_INIT_FINISHED) != 0;
    }

    public boolean isInitFailed() {
        return (flag & FLAG_MASK_INIT_FAILED) != 0;
    }

    public boolean isGroupReady() {
        return (flag & FLAG_MASK_GROUP_READY) != 0;
    }

    public boolean isBug() {
        return (flag & FLAG_BUG) != 0;
    }

    @Override
    public int actualSize() {
        if(size == 0) {
            size = PbUtil.sizeOfInt32Field(IDX_GROUP_ID, groupId) +
                    PbUtil.sizeOfInt32Field(IDX_NODE_ID, nodeId) +
                    PbUtil.sizeOfInt32Field(IDX_FLAG, flag) +
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
                    PbUtil.sizeOfFix32Field(IDX_PREPARED_OBSERVERS, preparedObservers) +
                    PbUtil.sizeOfFix64Field(IDX_LAST_CONFIG_CHANGE_INDEX, lastConfigChangeIndex) +
                    PbUtil.sizeOfUTF8(IDX_LAST_ERROR, lastError);
        }
        return size;
    }

    private static int[] toIntArray(Set<Integer> set) {
        if (set == null || set.isEmpty()) {
            return null;
        }
        int[] arr = new int[set.size()];
        int i = 0;
        for (Integer v : set) {
            arr[i++] = v;
        }
        return arr;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_GROUP_ID, groupId)) {
                    return false;
                }
                // fall through
            case IDX_GROUP_ID:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_NODE_ID, nodeId)) {
                    return false;
                }
                // fall through
            case IDX_NODE_ID:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_FLAG, flag)) {
                    return false;
                }
                // fall through
            case IDX_FLAG:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_TERM, term)) {
                    return false;
                }
                // fall through
            case IDX_TERM:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_LEADER_ID, leaderId)) {
                    return false;
                }
                // fall through
            case IDX_LEADER_ID:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_COMMIT_INDEX, commitIndex)) {
                    return false;
                }
                // fall through
            case IDX_COMMIT_INDEX:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_LAST_APPLIED, lastApplied)) {
                    return false;
                }
                // fall through
            case IDX_LAST_APPLIED:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_LAST_APPLY_TIME_TO_NOW_MILLIS, lastApplyTimeToNowMillis)) {
                    return false;
                }
                // fall through
            case IDX_LAST_APPLY_TIME_TO_NOW_MILLIS:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_LAST_LOG_INDEX, lastLogIndex)) {
                    return false;
                }
                // fall through
            case IDX_LAST_LOG_INDEX:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_APPLY_LAG_MILLIS, applyLagMillis)) {
                    return false;
                }
                // fall through
            case IDX_APPLY_LAG_MILLIS:
                if (!EncodeUtil.encodeFix32s(context, destBuffer, IDX_MEMBERS, toIntArray(members))) {
                    return false;
                }
                // fall through
            case IDX_MEMBERS:
                if (!EncodeUtil.encodeFix32s(context, destBuffer, IDX_OBSERVERS, toIntArray(observers))) {
                    return false;
                }
                // fall through
            case IDX_OBSERVERS:
                if (!EncodeUtil.encodeFix32s(context, destBuffer, IDX_PREPARED_MEMBERS, toIntArray(preparedMembers))) {
                    return false;
                }
                // fall through
            case IDX_PREPARED_MEMBERS:
                if (!EncodeUtil.encodeFix32s(context, destBuffer, IDX_PREPARED_OBSERVERS, toIntArray(preparedObservers))) {
                    return false;
                }
                // fall through
            case IDX_PREPARED_OBSERVERS:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_LAST_CONFIG_CHANGE_INDEX, lastConfigChangeIndex)) {
                    return false;
                }
                // fall through
            case IDX_LAST_CONFIG_CHANGE_INDEX:
                return EncodeUtil.encodeUTF8(context, destBuffer, IDX_LAST_ERROR, lastError);
            default:
                throw new CodecException(context);
        }
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
                case IDX_FLAG:
                    resp.flag = (int) value;
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
                case IDX_LAST_CONFIG_CHANGE_INDEX:
                    resp.lastConfigChangeIndex = value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == IDX_LAST_ERROR) {
                resp.lastError = parseUTF8(buf, fieldLen, currentPos);
            }
            return true;
        }

        @Override
        public QueryStatusResp getResult() {
            return resp;
        }
    }
}
