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
import com.github.dtprj.dongting.pb.PbUtil;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public abstract class WriteFrame extends Frame {

    private int dumpSize;

    protected abstract int estimateBodySize();

    protected abstract void encodeBody(ByteBuffer buf, ByteBufferPool pool);

    public int estimateSize() {
        int dumpSize = this.dumpSize;
        if (dumpSize == 0) {
            dumpSize = 4 // length
                    + 1 + 1 // uint32 frame_type = 1;
                    + 1 + 5 // uint32 command = 2;
                    + 1 + 4 // fixed32 seq = 3;
                    + 1 + 5 // uint32 resp_code = 4;
                    + PbUtil.maxStrSizeUTF8(msg) // string resp_msg = 5;
                    + 1 + 5 + estimateBodySize(); // bytes body = 15;
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
        encodeBody(buf, pool);
        buf.putInt(startPos, buf.position() - startPos - 4);
    }
}
