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
//  uint32 term = 1;
//  uint32 success = 2;
//  ///////////////////////////
//  uint32 append_code = 3;
//  uint32 suggest_term = 4;
//  fixed64 suggest_index = 5;
public class AppendResp extends RaftRpcData implements SimpleEncodable {
    // public int term;
    public boolean success;
    public int appendCode;
    public int suggestTerm;
    public long suggestIndex;

    @Override
    public int actualSize() {
        return PbUtil.sizeOfInt32Field(1, term)
                + PbUtil.sizeOfInt32Field(2, success ? 1 : 0)
                + PbUtil.sizeOfInt32Field(3, appendCode)
                + PbUtil.sizeOfInt32Field(4, suggestTerm)
                + PbUtil.sizeOfFix64Field(5, suggestIndex);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeInt32Field(buf, 1, term);
        PbUtil.writeInt32Field(buf, 2, success ? 1 : 0);
        PbUtil.writeInt32Field(buf, 3, appendCode);
        PbUtil.writeInt32Field(buf, 4, suggestTerm);
        PbUtil.writeFix64Field(buf, 5, suggestIndex);
    }

    // re-used
    public static class Callback extends PbCallback<AppendResp> {

        private AppendResp result;

        @Override
        protected void begin(int len) {
            result = new AppendResp();
        }

        @Override
        protected void end(boolean success) {
            result = null;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    result.term = (int) value;
                    break;
                case 2:
                    result.success = value != 0;
                    break;
                case 3:
                    result.appendCode = (int) value;
                    break;
                case 4:
                    result.suggestTerm = (int) value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            if (index == 5) {
                result.suggestIndex = value;
            }
            return true;
        }

        @Override
        public AppendResp getResult() {
            return result;
        }
    }
}
