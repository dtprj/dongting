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
package com.github.dtprj.dongting.remoting;

import com.github.dtprj.dongting.pb.PbUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public abstract class WriteFrame extends Frame {

    private int dumpSize;

    protected abstract int bodySize();

    protected abstract void encodeBody(ByteBuffer buf);

    public int estimateSize() {
        if (dumpSize == 0) {
            int msgBytes = msg == null ? 0 : msg.length() * 3 + (1 + 5);
            dumpSize = 4 // length
                    + (1 + 5) * 4 // first int32 field * 4
                    + msgBytes //msg
                    + bodySize() + (1 + 5); // body
        }
        return dumpSize;
    }

    public void encode(ByteBuffer buf) {
        int startPos = buf.position();
        buf.position(startPos + 4);
        if (frameType != 0) {
            PbUtil.writeTag(buf, PbUtil.TYPE_VAR_INT, Frame.IDX_TYPE);
            PbUtil.writeVarUnsignedInt32(buf, frameType);
        }
        if (command != 0) {
            PbUtil.writeTag(buf, PbUtil.TYPE_VAR_INT, Frame.IDX_COMMAND);
            PbUtil.writeVarUnsignedInt32(buf, command);
        }
        if (seq != 0) {
            PbUtil.writeTag(buf, PbUtil.TYPE_VAR_INT, Frame.IDX_SEQ);
            PbUtil.writeVarUnsignedInt32(buf, seq);
        }
        if (respCode != 0) {
            PbUtil.writeTag(buf, PbUtil.TYPE_VAR_INT, Frame.IDX_RESP_CODE);
            PbUtil.writeVarUnsignedInt32(buf, respCode);
        }
        if (msg != null && msg.length() > 0) {
            PbUtil.writeTag(buf, PbUtil.TYPE_LENGTH_DELIMITED, Frame.IDX_MSG);
            byte[] bs = msg.getBytes(StandardCharsets.UTF_8);
            PbUtil.writeVarUnsignedInt32(buf, bs.length);
            buf.put(bs);
        }
        encodeBody(buf);
        buf.putInt(startPos, buf.position() - startPos - 4);
    }
}
