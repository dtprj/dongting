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
//message TransferLeaderReq {
//uint32 group_id = 1;
//uint32 term = 2; // not used in admin transfer leader request
//uint32 old_leader_id = 3;
//uint32 new_leader_id = 4;
//fixed64 log_index = 5; // not used in admin transfer leader request
//}
public class TransferLeaderReq extends RaftRpcData implements SimpleEncodable {
    // public int groupId;
    // public int term;
    public int oldLeaderId;
    public int newLeaderId;
    public long logIndex;

    public TransferLeaderReq() {
    }

    @Override
    public int actualSize() {
        return PbUtil.accurateUnsignedIntSize(1, groupId)
                + PbUtil.accurateUnsignedIntSize(2, term)
                + PbUtil.accurateUnsignedIntSize(3, oldLeaderId)
                + PbUtil.accurateUnsignedIntSize(4, newLeaderId)
                + PbUtil.accurateFix64Size(5, logIndex);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeUnsignedInt32(buf, 1, groupId);
        PbUtil.writeUnsignedInt32(buf, 2, term);
        PbUtil.writeUnsignedInt32(buf, 3, oldLeaderId);
        PbUtil.writeUnsignedInt32(buf, 4, newLeaderId);
        PbUtil.writeFix64(buf, 5, logIndex);
    }

    static final class Callback extends PbCallback<TransferLeaderReq> {
        private final TransferLeaderReq req = new TransferLeaderReq();

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    req.groupId = (int) value;
                    break;
                case 2:
                    req.term = (int) value;
                    break;
                case 3:
                    req.oldLeaderId = (int) value;
                    break;
                case 4:
                    req.newLeaderId = (int) value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            if (index == 5) {
                req.logIndex = value;
            }
            return true;
        }

        @Override
        public TransferLeaderReq getResult() {
            return req;
        }
    }
}
