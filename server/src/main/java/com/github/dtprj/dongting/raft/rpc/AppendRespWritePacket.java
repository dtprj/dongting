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

import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.SmallNoCopyWritePacket;

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
public class AppendRespWritePacket extends SmallNoCopyWritePacket {
    private int term;
    private boolean success;
    private int appendCode;
    private int suggestTerm;
    private long suggestIndex;

    @Override
    protected int calcActualBodySize() {
        return PbUtil.accurateUnsignedIntSize(1, term)
                + PbUtil.accurateUnsignedIntSize(2, success ? 1 : 0)
                + PbUtil.accurateUnsignedIntSize(3, appendCode)
                + PbUtil.accurateUnsignedIntSize(4, suggestTerm)
                + PbUtil.accurateFix64Size(5, suggestIndex);
    }

    @Override
    protected void encodeBody(ByteBuffer buf) {
        PbUtil.writeUnsignedInt32(buf, 1, term);
        PbUtil.writeUnsignedInt32(buf, 2, success ? 1 : 0);
        PbUtil.writeUnsignedInt32(buf, 3, appendCode);
        PbUtil.writeUnsignedInt32(buf, 4, suggestTerm);
        PbUtil.writeFix64(buf, 5, suggestIndex);
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setAppendCode(int appendCode) {
        this.appendCode = appendCode;
    }

    public void setSuggestTerm(int suggestTerm) {
        this.suggestTerm = suggestTerm;
    }

    public void setSuggestIndex(long suggestIndex) {
        this.suggestIndex = suggestIndex;
    }
}
