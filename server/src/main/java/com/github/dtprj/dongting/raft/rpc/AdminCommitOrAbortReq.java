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
public class AdminCommitOrAbortReq extends PbCallback<AdminCommitOrAbortReq> implements SimpleEncodable {
    public int groupId;
    public long prepareIndex;

    public AdminCommitOrAbortReq() {
    }

    @Override
    public int actualSize() {
        return PbUtil.sizeOfInt32Field(1, groupId)
                + PbUtil.sizeOfFix64Field(2, prepareIndex);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeInt32Field(buf, 1, groupId);
        PbUtil.writeFix64Field(buf, 2, prepareIndex);
    }

    @Override
    protected AdminCommitOrAbortReq getResult() {
        return this;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        if (index == 1) {
            this.groupId = (int) value;
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        if (index == 2) {
            this.prepareIndex = value;
        }
        return true;
    }
}
