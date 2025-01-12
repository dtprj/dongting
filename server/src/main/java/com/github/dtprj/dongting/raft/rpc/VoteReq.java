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
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.codec.SimpleEncodable;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
//  uint32 group_id = 1;
//  uint32 term = 2;
//  uint32 candidate_id = 3;
//  fixed64 last_log_index = 4;
//  uint32 last_log_term = 5;
//  uint32 pre_vote = 6;
public class VoteReq implements SimpleEncodable {
    private int groupId;
    private int term;
    private int candidateId;
    private long lastLogIndex;
    private int lastLogTerm;
    private boolean preVote;

    @Override
    public int actualSize() {
        return PbUtil.accurateUnsignedLongSize(1, groupId)
                + PbUtil.accurateUnsignedLongSize(2, term)
                + PbUtil.accurateUnsignedIntSize(3, candidateId)
                + PbUtil.accurateFix64Size(4, lastLogIndex)
                + PbUtil.accurateUnsignedIntSize(5, lastLogTerm)
                + PbUtil.accurateUnsignedIntSize(6, preVote ? 1 : 0);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeUnsignedInt32(buf, 1, groupId);
        PbUtil.writeUnsignedInt32(buf, 2, term);
        PbUtil.writeUnsignedInt32(buf, 3, candidateId);
        PbUtil.writeFix64(buf, 4, lastLogIndex);
        PbUtil.writeUnsignedInt32(buf, 5, lastLogTerm);
        PbUtil.writeUnsignedInt32(buf, 6, preVote ? 1 : 0);
    }

    public static class Callback extends PbCallback<VoteReq> {
        private final VoteReq result = new VoteReq();

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
                    result.candidateId = (int) value;
                    break;
                case 5:
                    result.lastLogTerm = (int) value;
                    break;
                case 6:
                    result.preVote = value == 1;
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            if (index == 4) {
                result.lastLogIndex = value;
            }
            return true;
        }

        @Override
        public VoteReq getResult() {
            return result;
        }
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(int candidateId) {
        this.candidateId = candidateId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(int lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    public boolean isPreVote() {
        return preVote;
    }

    public void setPreVote(boolean preVote) {
        this.preVote = preVote;
    }

    public int getGroupId() {
        return groupId;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }
}
