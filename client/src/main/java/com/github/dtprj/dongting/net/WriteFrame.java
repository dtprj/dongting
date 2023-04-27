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

/**
 * @author huangli
 */
public abstract class WriteFrame extends Frame {

    private int dumpSize;
    private int bodySize;

    protected abstract int calcEstimateBodySize();

    protected abstract void encodeBody(ByteBuffer buf, ByteBufferPool pool);

    public final int estimateBodySize() {
        if (bodySize == 0) {
            bodySize = calcEstimateBodySize();
        }
        return bodySize;
    }

    public int estimateSize() {
        int dumpSize = this.dumpSize;
        if (dumpSize == 0) {
            dumpSize = 4 // length
                    + 1 + 1 // uint32 frame_type = 1;
                    + 1 + 5 // uint32 command = 2;
                    + 1 + 4 // fixed32 seq = 3;
                    + 1 + 5 // uint32 resp_code = 4;
                    + PbUtil.maxStrSizeUTF8(msg) // string resp_msg = 5;
                    + 1 + 8; // fixed32 timeout_millis = 6;
            int bodySize = estimateBodySize();
            if (bodySize > 0) {
                dumpSize += 1 + 5 + bodySize; // bytes body = 15;
            }
            this.dumpSize = dumpSize;
        }
        return dumpSize;
    }

    public void encode(ByteBuffer buf, ByteBufferPool pool) {
        int startPos = buf.position();
        buf.position(startPos + 4);
        PbUtil.writeUnsignedInt32(buf, Frame.IDX_TYPE, frameType);
        PbUtil.writeUnsignedInt32(buf, Frame.IDX_COMMAND, command);
        PbUtil.writeFix32(buf, Frame.IDX_SEQ, seq);
        PbUtil.writeUnsignedInt32(buf, Frame.IDX_RESP_CODE, respCode);
        PbUtil.writeUTF8(buf, Frame.IDX_MSG, msg);
        PbUtil.writeFix64(buf, Frame.IDX_TIMOUT, timeout);
        encodeBody(buf, pool);
        buf.putInt(startPos, buf.position() - startPos - 4);
    }

    protected void writeBodySize(ByteBuffer buf, int len) {
        PbUtil.writeLengthDelimitedPrefix(buf, Frame.IDX_BODY, len, true);
    }
}
