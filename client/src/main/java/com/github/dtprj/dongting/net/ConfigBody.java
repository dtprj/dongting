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
    int maxPacketSize;
    int maxBodySize;
    int maxInPending;
    long maxInPendingBytes;
    int maxOutPending;
    long maxOutPendingBytes;

    @Override
    public boolean readVarNumber(int index, long value) {
        switch (index) {
            case 1:
                maxPacketSize = (int) value;
                break;
            case 2:
                maxBodySize = (int) value;
                break;
            case 3:
                maxInPending = (int) value;
                break;
            case 4:
                maxInPendingBytes = value;
                break;
            case 5:
                maxOutPending = (int) value;
                break;
            case 6:
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
        return PbUtil.sizeOfInt32Field(1, maxPacketSize) +
                PbUtil.sizeOfInt32Field(2, maxBodySize) +
                PbUtil.sizeOfInt32Field(3, maxInPending) +
                PbUtil.sizeOfInt64Field(4, maxInPendingBytes) +
                PbUtil.sizeOfInt32Field(5, maxOutPending) +
                PbUtil.sizeOfInt64Field(6, maxOutPendingBytes);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeInt32Field(buf, 1, maxPacketSize);
        PbUtil.writeInt32Field(buf, 2, maxBodySize);
        PbUtil.writeInt32Field(buf, 3, maxInPending);
        PbUtil.writeInt64Field(buf, 4, maxInPendingBytes);
        PbUtil.writeInt32Field(buf, 5, maxOutPending);
        PbUtil.writeInt64Field(buf, 6, maxOutPendingBytes);
    }
}
