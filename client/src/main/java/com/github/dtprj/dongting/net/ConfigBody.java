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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.codec.SimpleEncodable;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class ConfigBody extends PbCallback<ConfigBody> implements SimpleEncodable {

    private static final int IDX_MAX_PACKET_SIZE = 1;
    private static final int IDX_MAX_BODY_SIZE = 2;
    private static final int IDX_MAX_IN_PENDING = 3;
    private static final int IDX_MAX_IN_PENDING_BYTES = 4;
    private static final int IDX_MAX_OUT_PENDING = 5;
    private static final int IDX_MAX_OUT_PENDING_BYTES = 6;

    public int maxPacketSize;
    public int maxBodySize;
    public int maxInPending;
    public long maxInPendingBytes;
    public int maxOutPending;
    public long maxOutPendingBytes;

    @Override
    public boolean readVarNumber(int index, long value) {
        switch (index) {
            case IDX_MAX_PACKET_SIZE:
                maxPacketSize = (int) value;
                break;
            case IDX_MAX_BODY_SIZE:
                maxBodySize = (int) value;
                break;
            case IDX_MAX_IN_PENDING:
                maxInPending = (int) value;
                break;
            case IDX_MAX_IN_PENDING_BYTES:
                maxInPendingBytes = value;
                break;
            case IDX_MAX_OUT_PENDING:
                maxOutPending = (int) value;
                break;
            case IDX_MAX_OUT_PENDING_BYTES:
                maxOutPendingBytes = value;
                break;
        }
        return true;
    }

    @Override
    protected ConfigBody getResult() {
        return this;
    }

    @Override
    public int actualSize() {
        return PbUtil.sizeOfInt32Field(IDX_MAX_PACKET_SIZE, maxPacketSize) +
                PbUtil.sizeOfInt32Field(IDX_MAX_BODY_SIZE, maxBodySize) +
                PbUtil.sizeOfInt32Field(IDX_MAX_IN_PENDING, maxInPending) +
                PbUtil.sizeOfInt64Field(IDX_MAX_IN_PENDING_BYTES, maxInPendingBytes) +
                PbUtil.sizeOfInt32Field(IDX_MAX_OUT_PENDING, maxOutPending) +
                PbUtil.sizeOfInt64Field(IDX_MAX_OUT_PENDING_BYTES, maxOutPendingBytes);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeInt32Field(buf, IDX_MAX_PACKET_SIZE, maxPacketSize);
        PbUtil.writeInt32Field(buf, IDX_MAX_BODY_SIZE, maxBodySize);
        PbUtil.writeInt32Field(buf, IDX_MAX_IN_PENDING, maxInPending);
        PbUtil.writeInt64Field(buf, IDX_MAX_IN_PENDING_BYTES, maxInPendingBytes);
        PbUtil.writeInt32Field(buf, IDX_MAX_OUT_PENDING, maxOutPending);
        PbUtil.writeInt64Field(buf, IDX_MAX_OUT_PENDING_BYTES, maxOutPendingBytes);
    }
}
