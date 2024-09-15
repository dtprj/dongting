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

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class ConfigBody {
    int maxPacketSize;
    int maxBodySize;
    int maxInPending;
    long maxInPendingBytes;
    int maxOutPending;
    long maxOutPendingBytes;

    public static class Callback extends PbCallback<ConfigBody> {
        final ConfigBody result = new ConfigBody();

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    result.maxPacketSize = (int) value;
                    break;
                case 2:
                    result.maxBodySize = (int) value;
                    break;
                case 3:
                    result.maxInPending = (int) value;
                    break;
                case 4:
                    result.maxInPendingBytes = value;
                    break;
                case 5:
                    result.maxOutPending = (int) value;
                    break;
                case 6:
                    result.maxOutPendingBytes = value;
                    break;
            }
            return true;
        }

        @Override
        protected ConfigBody getResult() {
            return result;
        }
    }

    public int calcActualBodySize() {
        return PbUtil.accurateUnsignedIntSize(1, maxPacketSize) +
                PbUtil.accurateUnsignedIntSize(2, maxBodySize) +
                PbUtil.accurateUnsignedIntSize(3, maxInPending) +
                PbUtil.accurateUnsignedLongSize(4, maxInPendingBytes) +
                PbUtil.accurateUnsignedIntSize(5, maxOutPending) +
                PbUtil.accurateUnsignedLongSize(6, maxOutPendingBytes);
    }

    public void encodeBody(ByteBuffer buf) {
        PbUtil.writeUnsignedInt32(buf, 1, maxPacketSize);
        PbUtil.writeUnsignedInt32(buf, 2, maxBodySize);
        PbUtil.writeUnsignedInt32(buf, 3, maxInPending);
        PbUtil.writeUnsignedInt64(buf, 4, maxInPendingBytes);
        PbUtil.writeUnsignedInt32(buf, 5, maxOutPending);
        PbUtil.writeUnsignedInt64(buf, 6, maxOutPendingBytes);
    }

    public int getMaxPacketSize() {
        return maxPacketSize;
    }

    public int getMaxBodySize() {
        return maxBodySize;
    }

    public int getMaxInPending() {
        return maxInPending;
    }

    public long getMaxInPendingBytes() {
        return maxInPendingBytes;
    }

    public int getMaxOutPending() {
        return maxOutPending;
    }

    public long getMaxOutPendingBytes() {
        return maxOutPendingBytes;
    }
}
