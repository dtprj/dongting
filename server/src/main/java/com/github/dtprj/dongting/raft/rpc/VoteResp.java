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

import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.SmallNoCopyWriteFrame;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
//  uint32 term = 1;
//  uint32 vote_granted = 2;
public class VoteResp {
    private int term;
    private boolean voteGranted;

    public static class Callback extends PbCallback<VoteResp> {
        private final VoteResp result = new VoteResp();

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    result.term = (int) value;
                    break;
                case 2:
                    result.voteGranted = value != 0;
                    break;
            }
            return true;
        }

        @Override
        public VoteResp getResult() {
            return result;
        }
    }

    public static class VoteRespWriteFrame extends SmallNoCopyWriteFrame {

        private final VoteResp data;

        public VoteRespWriteFrame(VoteResp data) {
            this.data = data;
        }

        @Override
        protected int calcActualBodySize(EncodeContext context) {
            return PbUtil.accurateUnsignedIntSize(1, data.term)
                    + PbUtil.accurateUnsignedIntSize(2, data.voteGranted ? 1 : 0);
        }

        @Override
        protected void encodeBody(ByteBuffer buf) {
            PbUtil.writeUnsignedInt32(buf, 1, data.term);
            PbUtil.writeUnsignedInt32(buf, 2, data.voteGranted ? 1 : 0);
        }
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }
}
