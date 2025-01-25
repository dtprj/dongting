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

import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.codec.SimpleEncodable;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class HandshakeBody extends PbCallback<HandshakeBody> implements SimpleEncodable {

    public static final long MAGIC1 = 0xAE10_9045_1C22_DA13L;
    public static final long MAGIC2 = 0x1CD7_D1A3_0A61_935FL;

    int majorVersion;
    int minorVersion;
    ConfigBody config;

    @Override
    protected HandshakeBody getResult() {
        return this;
    }

    @Override
    public boolean readFix64(int index, long value) {
        switch (index) {
            case 1:
                if (value != MAGIC1) {
                    throw new NetException("handshake failed, magic1 not match");
                }
                break;
            case 2:
                if (value != MAGIC2) {
                    throw new NetException("handshake failed, magic2 not match");
                }
                break;
        }
        return true;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        switch (index) {
            case 3:
                majorVersion = (int) value;
                break;
            case 4:
                minorVersion = (int) value;
                break;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index == 8) {
            ConfigBody nestedCallback = currentPos == 0 ? new ConfigBody() : null;
            config = parseNested(buf, fieldLen, currentPos, nestedCallback);
        }
        return true;
    }

    @Override
    public int actualSize() {
        return PbUtil.accurateFix64Size(1, MAGIC1)
                + PbUtil.accurateFix64Size(2, MAGIC2)
                + PbUtil.accurateUnsignedIntSize(3, majorVersion)
                + PbUtil.accurateUnsignedIntSize(4, minorVersion)
                + EncodeUtil.actualSize(8, config);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeFix64(buf, 1, MAGIC1);
        PbUtil.writeFix64(buf, 2, MAGIC2);
        PbUtil.writeUnsignedInt32(buf, 3, majorVersion);
        PbUtil.writeUnsignedInt32(buf, 4, minorVersion);
        if (config != null) {
            PbUtil.writeLengthDelimitedPrefix(buf, 8, config.actualSize());
            config.encode(buf);
        }
    }
}
