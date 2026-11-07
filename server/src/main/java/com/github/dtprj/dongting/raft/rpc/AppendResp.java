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

import com.github.dtprj.dongting.codec.DecodeContext;
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
    private static final int IDX_TERM = 1;
    private static final int IDX_SUCCESS = 2;
    private static final int IDX_APPEND_CODE = 3;
    private static final int IDX_SUGGEST_TERM = 4;
    private static final int IDX_SUGGEST_INDEX = 5;

    // public int term;
    public boolean success;
    public int appendCode;
    public int suggestTerm;
    public long suggestIndex;

    @Override
    public int actualSize() {
        return PbUtil.sizeOfInt32Field(IDX_TERM, term)
                + PbUtil.sizeOfInt32Field(IDX_SUCCESS, success ? 1 : 0)
                + PbUtil.sizeOfInt32Field(IDX_APPEND_CODE, appendCode)
                + PbUtil.sizeOfInt32Field(IDX_SUGGEST_TERM, suggestTerm)
                + PbUtil.sizeOfFix64Field(IDX_SUGGEST_INDEX, suggestIndex);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeInt32Field(buf, IDX_TERM, term);
        PbUtil.writeInt32Field(buf, IDX_SUCCESS, success ? 1 : 0);
        PbUtil.writeInt32Field(buf, IDX_APPEND_CODE, appendCode);
        PbUtil.writeInt32Field(buf, IDX_SUGGEST_TERM, suggestTerm);
        PbUtil.writeFix64Field(buf, IDX_SUGGEST_INDEX, suggestIndex);
    }

    public static class Callback extends AppendResp implements PbCallback<AppendResp> {

        @Override
        public void readVarNumber(DecodeContext context, int index, long value) {
            switch (index) {
                case IDX_TERM:
                    term = (int) value;
                    break;
                case IDX_SUCCESS:
                    success = value != 0;
                    break;
                case IDX_APPEND_CODE:
                    appendCode = (int) value;
                    break;
                case IDX_SUGGEST_TERM:
                    suggestTerm = (int) value;
                    break;
            }
        }

        @Override
        public void readFix64(DecodeContext context, int index, long value) {
            if (index == IDX_SUGGEST_INDEX) {
                suggestIndex = value;
            }
        }

        @Override
        public AppendResp getResult(DecodeContext context) {
            return this;
        }
    }
}
