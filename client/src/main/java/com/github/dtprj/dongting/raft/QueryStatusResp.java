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

/**
 * @author huangli
 */
public class QueryStatusResp extends RaftConfigRpcData implements SimpleEncodable {
    // uint32 group_id = 1;
    // uint32 term = 2;
    // uint32 leader_id = 3;
    // fixed64 commit_index = 4;
    // fixed64 last_applied = 5;
    // fixed64 last_log_index = 6;
    // repeated fixed32 members = 7[packed = false];
    // repeated fixed32 observers = 8[packed = false];
    // repeated fixed32 prepared_members = 9[packed = false];
    // repeated fixed32 prepared_observers = 10[packed = false];

    private int leaderId;
    private long commitIndex;
    private long lastApplied;
    private long lastLogIndex;

    public static final DecoderCallbackCreator<QueryStatusResp> DECODER = ctx -> ctx.toDecoderCallback(
            new Callback());

    @Override
    public int actualSize() {
        return PbUtil.accurateUnsignedIntSize(1, groupId) +
                PbUtil.accurateUnsignedIntSize(2, term) +
                PbUtil.accurateUnsignedIntSize(3, leaderId) +
                PbUtil.accurateFix64Size(4, commitIndex) +
                PbUtil.accurateFix64Size(5, lastApplied) +
                PbUtil.accurateFix64Size(6, lastLogIndex) +
                PbUtil.accurateFix32Size(7, members) +
                PbUtil.accurateFix32Size(8, observers) +
                PbUtil.accurateFix32Size(9, preparedMembers) +
                PbUtil.accurateFix32Size(10, preparedObservers);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeUnsignedInt32(buf, 1, groupId);
        PbUtil.writeUnsignedInt32(buf, 2, term);
        PbUtil.writeUnsignedInt32(buf, 3, leaderId);
        PbUtil.writeFix64(buf, 4, commitIndex);
        PbUtil.writeFix64(buf, 5, lastApplied);
        PbUtil.writeFix64(buf, 6, lastLogIndex);
        PbUtil.writeFix32(buf, 7, members);
        PbUtil.writeFix32(buf, 8, observers);
        PbUtil.writeFix32(buf, 9, preparedMembers);
        PbUtil.writeFix32(buf, 10, preparedObservers);
    }

    public static final class Callback extends PbCallback<QueryStatusResp> {
        private final QueryStatusResp resp = new QueryStatusResp();

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    resp.groupId = (int) value;
                    break;
                case 2:
                    resp.term = (int) value;
                    break;
                case 3:
                    resp.leaderId = (int) value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix32(int index, int value) {
            switch (index) {
                case 7:
                    resp.members.add(value);
                    break;
                case 8:
                    resp.observers.add(value);
                    break;
                case 9:
                    resp.preparedMembers.add(value);
                    break;
                case 10:
                    resp.preparedObservers.add(value);
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case 4:
                    resp.commitIndex = value;
                    break;
                case 5:
                    resp.lastApplied = value;
                    break;
                case 6:
                    resp.lastLogIndex = value;
                    break;
            }
            return true;
        }

        @Override
        public QueryStatusResp getResult() {
            return resp;
        }
    }

    public int getLeaderId() {
        return leaderId;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }
}
