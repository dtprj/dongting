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
import com.github.dtprj.dongting.raft.RaftRpcData;

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
public class VoteReq extends RaftRpcData implements SimpleEncodable {
    // public int groupId;
    // public int term;
    public int candidateId;
    public long lastLogIndex;
    public int lastLogTerm;
    public boolean preVote;

    @Override
    public int actualSize() {
        return PbUtil.sizeOfInt64Field(1, groupId)
                + PbUtil.sizeOfInt64Field(2, term)
                + PbUtil.sizeOfInt32Field(3, candidateId)
                + PbUtil.sizeOfFix64Field(4, lastLogIndex)
                + PbUtil.sizeOfInt32Field(5, lastLogTerm)
                + PbUtil.sizeOfInt32Field(6, preVote ? 1 : 0);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeInt32Field(buf, 1, groupId);
        PbUtil.writeInt32Field(buf, 2, term);
        PbUtil.writeInt32Field(buf, 3, candidateId);
        PbUtil.writeFix64Field(buf, 4, lastLogIndex);
        PbUtil.writeInt32Field(buf, 5, lastLogTerm);
        PbUtil.writeInt32Field(buf, 6, preVote ? 1 : 0);
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

}
