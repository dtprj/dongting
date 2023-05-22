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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.codec.PbUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public abstract class WriteFrame extends Frame {

    private int dumpSize;
    private int bodySize;

    private byte[] msgBytes;

    protected abstract int calcActualBodySize();

    protected abstract void encodeBody(ByteBuffer buf, ByteBufferPool pool);

    public final int actualBodySize() {
        if (bodySize == 0) {
            bodySize = calcActualBodySize();
        }
        return bodySize;
    }

    public int actualSize() {
        int dumpSize = this.dumpSize;
        if (dumpSize == 0) {
            if (msg != null && !msg.isEmpty()) {
                msgBytes = msg.getBytes(StandardCharsets.UTF_8);
            }
            dumpSize = 4 // length
                    + PbUtil.accurateUnsignedIntSize(1, frameType) // uint32 frame_type = 1;
                    + PbUtil.accurateUnsignedIntSize(2, command) // uint32 command = 2;
                    + PbUtil.accurateFix32Size(3, seq) // fixed32 seq = 3;
                    + PbUtil.accurateUnsignedIntSize(4, respCode) // uint32 resp_code = 4;
                    + PbUtil.accurateLengthDelimitedSize(5, msgBytes == null ? 0 : msgBytes.length) // string resp_msg = 5;
                    + PbUtil.accurateFix64Size(6, timeout) // fixed64 timeout = 6;
                    + PbUtil.accurateLengthDelimitedSize(15, actualBodySize()); // bytes body = 15;
            this.dumpSize = dumpSize;
        }
        return dumpSize;
    }

    public void encode(ByteBuffer buf, ByteBufferPool pool) {
        buf.putInt(actualSize());
        PbUtil.writeUnsignedInt32(buf, Frame.IDX_TYPE, frameType);
        PbUtil.writeUnsignedInt32(buf, Frame.IDX_COMMAND, command);
        PbUtil.writeFix32(buf, Frame.IDX_SEQ, seq);
        PbUtil.writeUnsignedInt32(buf, Frame.IDX_RESP_CODE, respCode);
        PbUtil.writeUTF8(buf, Frame.IDX_MSG, msg);
        PbUtil.writeFix64(buf, Frame.IDX_TIMOUT, timeout);
        if (actualBodySize() > 0) {
            PbUtil.writeLengthDelimitedPrefix(buf, Frame.IDX_BODY, actualBodySize());
            encodeBody(buf, pool);
        }
    }

}
