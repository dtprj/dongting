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
public class HandshakeBody {

    public static final long MAGIC1 = 0xAE10_9045_1C22_DA13L;
    public static final long MAGIC2 = 0x1CD7_D1A3_0A61_935FL;

    long magic1;
    long magic2;
    int majorVersion;
    int minorVersion;
    ConfigBody config;

    public static class Callback extends PbCallback<HandshakeBody> {
        private final HandshakeBody result = new HandshakeBody();

        @Override
        protected HandshakeBody getResult() {
            return result;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    result.magic1 = value;
                    break;
                case 2:
                    result.magic2 = value;
                    break;
                case 3:
                    result.majorVersion = (int) value;
                    break;
                case 4:
                    result.minorVersion = (int) value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == 8) {
                ConfigBody.Callback nestedCallback = currentPos == 0 ? new ConfigBody.Callback() : null;
                result.config = parseNested(buf, fieldLen, currentPos, nestedCallback);
            }
            return true;
        }
    }

    public static class HandshakeBodyWritePacket extends SmallNoCopyWritePacket {

        private final HandshakeBody v;
        private final int configBodySize;

        public HandshakeBodyWritePacket(HandshakeBody v) {
            this.v = v;
            configBodySize = v.config == null ? 0 : v.config.calcActualBodySize();
        }

        @Override
        protected int calcActualBodySize() {
            return PbUtil.accurateFix64Size(1, MAGIC1)
                    + PbUtil.accurateFix64Size(2, MAGIC2)
                    + PbUtil.accurateUnsignedIntSize(3, v.majorVersion)
                    + PbUtil.accurateUnsignedIntSize(4, v.minorVersion)
                    + PbUtil.accurateLengthDelimitedSize(8, configBodySize);
        }

        @Override
        protected void encodeBody(ByteBuffer buf) {
            PbUtil.writeFix64(buf, 1, MAGIC1);
            PbUtil.writeFix64(buf, 2, MAGIC2);
            PbUtil.writeUnsignedInt32(buf, 3, v.majorVersion);
            PbUtil.writeUnsignedInt32(buf, 4, v.minorVersion);
            if (configBodySize > 0) {
                PbUtil.writeLengthDelimitedPrefix(buf, 8, configBodySize);
                v.config.encodeBody(buf);
            }
        }
    }
}
