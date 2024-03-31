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

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.SmallNoCopyWriteFrame;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class QueryStatusResp {
    // uint32 group_id = 1;
    // uint32 term = 2;
    // uint32 leader_id = 3;
    // fixed64 commit_index = 4;
    // fixed64 last_applied = 5;
    // fixed64 last_log_index = 6;
    private int groupId;
    private int term;
    private int leaderId;
    private long commitIndex;
    private long lastApplied;
    private long lastLogIndex;

    public static class QueryStatusRespCallback extends PbCallback<QueryStatusResp> {
        private final QueryStatusResp result = new QueryStatusResp();

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    result.groupId = (int) value;
                    break;
                case 2:
                    result.term = (int) value;
                    break;
                case 3:
                    result.leaderId = (int) value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case 4:
                    result.commitIndex = value;
                    break;
                case 5:
                    result.lastApplied = value;
                    break;
                case 6:
                    result.lastLogIndex = value;
                    break;
            }
            return true;
        }

        @Override
        public QueryStatusResp getResult() {
            return result;
        }
    }

    public static class QueryStatusRespWriteFrame extends SmallNoCopyWriteFrame {

        QueryStatusResp resp;

        public QueryStatusRespWriteFrame(QueryStatusResp resp) {
            this.resp = resp;
        }

        @Override
        protected int calcActualBodySize() {
            return PbUtil.accurateUnsignedIntSize(1, resp.groupId) +
                    PbUtil.accurateUnsignedIntSize(2, resp.term) +
                    PbUtil.accurateUnsignedIntSize(3, resp.leaderId) +
                    PbUtil.accurateFix64Size(4, resp.commitIndex) +
                    PbUtil.accurateFix64Size(5, resp.lastApplied) +
                    PbUtil.accurateFix64Size(6, resp.lastLogIndex);
        }

        @Override
        protected void encodeBody(ByteBuffer buf) {
            PbUtil.writeUnsignedInt32(buf, 1, resp.groupId);
            PbUtil.writeUnsignedInt32(buf, 2, resp.term);
            PbUtil.writeUnsignedInt32(buf, 3, resp.leaderId);
            PbUtil.writeFix64(buf, 4, resp.commitIndex);
            PbUtil.writeFix64(buf, 5, resp.lastApplied);
            PbUtil.writeFix64(buf, 6, resp.lastLogIndex);
        }
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

    public long getCommitIndex() {
        return commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    public void setTerm(int term) {
        this.term = term;
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
