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
class HandshakeBody extends PbCallback<HandshakeBody> implements SimpleEncodable {

    public static final long MAGIC1 = 0xAE10_9045_1C22_DA13L;
    public static final long MAGIC2 = 0x1CD7_D1A3_0A61_935FL;

    private static final int IDX_MAGIC1 = 1;
    private static final int IDX_MAGIC2 = 2;
    private static final int IDX_MAJOR_VERSION = 3;
    private static final int IDX_MINOR_VERSION = 4;
    private static final int IDX_PROCESS_INFO = 5;
    private static final int IDX_CONFIG = 8;

    int majorVersion;
    int minorVersion;
    ProcessInfoBody processInfo;
    ConfigBody config;

    @Override
    protected HandshakeBody getResult() {
        return this;
    }

    @Override
    public boolean readFix64(int index, long value) {
        switch (index) {
            case IDX_MAGIC1:
                if (value != MAGIC1) {
                    throw new NetException("handshake failed, magic1 not match");
                }
                break;
            case IDX_MAGIC2:
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
            case IDX_MAJOR_VERSION:
                majorVersion = (int) value;
                break;
            case IDX_MINOR_VERSION:
                minorVersion = (int) value;
                break;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index == IDX_CONFIG) {
            ConfigBody nestedCallback = currentPos == 0 ? new ConfigBody() : null;
            config = parseNested(buf, fieldLen, currentPos, nestedCallback);
        } else if (index == IDX_PROCESS_INFO) {
            ProcessInfoBody nestedCallback = currentPos == 0 ? new ProcessInfoBody() : null;
            processInfo = parseNested(buf, fieldLen, currentPos, nestedCallback);
        }
        return true;
    }

    @Override
    public int actualSize() {
        return PbUtil.sizeOfFix64Field(IDX_MAGIC1, MAGIC1)
                + PbUtil.sizeOfFix64Field(IDX_MAGIC2, MAGIC2)
                + PbUtil.sizeOfInt32Field(IDX_MAJOR_VERSION, majorVersion)
                + PbUtil.sizeOfInt32Field(IDX_MINOR_VERSION, minorVersion)
                + EncodeUtil.sizeOf(IDX_PROCESS_INFO, processInfo)
                + EncodeUtil.sizeOf(IDX_CONFIG, config);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeFix64Field(buf, IDX_MAGIC1, MAGIC1);
        PbUtil.writeFix64Field(buf, IDX_MAGIC2, MAGIC2);
        PbUtil.writeInt32Field(buf, IDX_MAJOR_VERSION, majorVersion);
        PbUtil.writeInt32Field(buf, IDX_MINOR_VERSION, minorVersion);
        EncodeUtil.encode(buf, IDX_PROCESS_INFO, processInfo);
        EncodeUtil.encode(buf, IDX_CONFIG, config);
    }
}

class ProcessInfoBody extends PbCallback<ProcessInfoBody> implements SimpleEncodable {

    private static final int IDX_UUID1 = 1;
    private static final int IDX_UUID2 = 2;

    long uuid1;
    long uuid2;

    @Override
    protected ProcessInfoBody getResult() {
        return this;
    }

    @Override
    public boolean readFix64(int index, long value) {
        switch (index) {
            case IDX_UUID1:
                uuid1 = value;
                break;
            case IDX_UUID2:
                uuid2 = value;
                break;
        }
        return true;
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeFix64Field(buf, IDX_UUID1, uuid1);
        PbUtil.writeFix64Field(buf, IDX_UUID2, uuid2);
    }

    @Override
    public int actualSize() {
        return PbUtil.sizeOfFix64Field(IDX_UUID1, uuid1) +
                PbUtil.sizeOfFix64Field(IDX_UUID2, uuid2);
    }
}
