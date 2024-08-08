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
    int maxFrameSize;
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
                    result.maxFrameSize = (int) value;
                    break;
                case 2:
                    result.maxBodySize = (int) value;
                    break;
                case 3:
                    result.maxInPending = (int) value;
                    break;
                case 5:
                    result.maxOutPending = (int) value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case 4:
                    result.maxInPendingBytes = value;
                    break;
                case 6:
                    result.maxOutPendingBytes = value;
                    break;
            }
            return true;
        }
    }

    public int calcActualBodySize(){
        return PbUtil.accurateUnsignedIntSize(1, maxFrameSize) +
                PbUtil.accurateUnsignedIntSize(2, maxBodySize) +
                PbUtil.accurateUnsignedIntSize(3, maxInPending) +
                PbUtil.accurateFix64Size(4, maxInPendingBytes) +
                PbUtil.accurateUnsignedIntSize(5, maxOutPending) +
                PbUtil.accurateFix64Size(6, maxOutPendingBytes);
    }

    public void encodeBody(ByteBuffer buf) {
        PbUtil.writeUnsignedInt32(buf, 1, maxFrameSize);
        PbUtil.writeUnsignedInt32(buf, 2, maxBodySize);
        PbUtil.writeUnsignedInt32(buf, 3, maxInPending);
        PbUtil.writeFix64(buf, 4, maxInPendingBytes);
        PbUtil.writeUnsignedInt32(buf, 5, maxOutPending);
        PbUtil.writeFix64(buf, 6, maxOutPendingBytes);
    }
}
