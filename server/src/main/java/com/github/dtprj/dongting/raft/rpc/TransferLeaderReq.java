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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
//message TransferLeaderReq {
//  uint32 group_id = 1;
//  uint32 term = 2;
//  uint32 old_leader_id = 3;
//  fixed64 log_index = 4;
//}
public class TransferLeaderReq {
    public int groupId;
    public int term;
    public int oldLeaderId;
    public long logIndex;

    public static class Callback extends PbCallback<TransferLeaderReq> {
        private final TransferLeaderReq result = new TransferLeaderReq();

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
                    result.oldLeaderId = (int) value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            if (index == 4) {
                result.logIndex = value;
            }
            return true;
        }

        @Override
        public TransferLeaderReq getResult() {
            return result;
        }
    }

    public static class WriteFrame extends com.github.dtprj.dongting.net.WriteFrame {
        private final TransferLeaderReq req;

        public WriteFrame(TransferLeaderReq req) {
            this.req = req;
        }

        @Override
        protected int calcEstimateBodySize() {
            return PbUtil.accurateUnsignedIntSize(1, req.groupId)
                    + PbUtil.accurateUnsignedIntSize(2, req.term)
                    + PbUtil.accurateUnsignedIntSize(3, req.oldLeaderId)
                    + PbUtil.accurateFix64Size(4, req.logIndex);
        }

        @Override
        protected void encodeBody(ByteBuffer buf, ByteBufferPool pool) {
            PbUtil.writeUnsignedInt32(buf, 1, req.groupId);
            PbUtil.writeUnsignedInt32(buf, 2, req.term);
            PbUtil.writeUnsignedInt32(buf, 3, req.oldLeaderId);
            PbUtil.writeFix64(buf, 4, req.logIndex);
        }
    }
}
