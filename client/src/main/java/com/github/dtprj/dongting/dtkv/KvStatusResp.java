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
package com.github.dtprj.dongting.dtkv;

import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.codec.SimpleEncodable;
import com.github.dtprj.dongting.raft.QueryStatusResp;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class KvStatusResp extends PbCallback<KvStatusResp> implements SimpleEncodable {
    public static final int IDX_RAFT_SERVER_STATUS = 1;
    public static final int IDX_WATCH_COUNT = 2;

    public QueryStatusResp raftServerStatus;
    public int watchCount;

    public KvStatusResp() {
    }

    @Override
    protected KvStatusResp getResult() {
        return this;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        if (index == IDX_WATCH_COUNT) {
            this.watchCount = (int) value;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index == IDX_RAFT_SERVER_STATUS) {
            raftServerStatus = parseNested(buf, fieldLen, currentPos,
                    currentPos == 0 ? new QueryStatusResp.Callback() : null);
            return true;
        }
        return true;
    }

    @Override
    public void encode(ByteBuffer destBuffer) {
        EncodeUtil.encode(destBuffer, IDX_RAFT_SERVER_STATUS, raftServerStatus);
        PbUtil.writeInt32Field(destBuffer, IDX_WATCH_COUNT, watchCount);
    }

    @Override
    public int actualSize() {
        return EncodeUtil.sizeOf(IDX_RAFT_SERVER_STATUS, raftServerStatus)
                + PbUtil.sizeOfInt32Field(IDX_WATCH_COUNT, watchCount);
    }
}
