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
public class Frame {
    public static final int IDX_TYPE = 1;
    public static final int IDX_COMMAND = 2;
    public static final int IDX_SEQ = 3;
    public static final int IDX_RESP_CODE = 4;
    public static final int IDX_MSG = 5;
    public static final int IDX_BODY = 15;

    private int frameType;
    private int command;
    private int seq;
    private int respCode;
    private String msg;
    private ByteBuffer body;

    private int pbSize;

    public int pbSize() {
        if (pbSize == 0) {
            int msgBytes = msg == null ? 0 : msg.length() * 3 + (1 + 5);
            int bodyBytes = body == null ? 0 : body.remaining() + (1 + 5);
            pbSize = (1 + 5) * 4 // first int32 field * 4
                    + msgBytes //msg
                    + bodyBytes; // body
        }
        return pbSize;
    }

    public ByteBuffer toByteBuffer() {
        // TODO need optimise
        int s = pbSize();
        ByteBuffer buf = ByteBuffer.allocate(s + 4);
        buf.position(4);
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
        if (body != null && body.remaining() > 0) {
            PbUtil.writeTag(buf, PbUtil.TYPE_LENGTH_DELIMITED, Frame.IDX_BODY);
            PbUtil.writeVarUnsignedInt32(buf, body.remaining());
            body.mark();
            buf.put(body);
            body.reset();
        }
        buf.putInt(0, buf.position() - 4);
        buf.flip();
        return buf;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Frame(type=").append(frameType)
                .append(",cmd=").append(command)
                .append(",seq=").append(seq)
                .append(",respCode=").append(respCode)
                .append(")");
        return sb.toString();
    }


    public int getFrameType() {
        return frameType;
    }

    public void setFrameType(int frameType) {
        this.frameType = frameType;
    }

    public int getCommand() {
        return command;
    }

    public void setCommand(int command) {
        this.command = command;
    }

    public int getSeq() {
        return seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }

    public int getRespCode() {
        return respCode;
    }

    public void setRespCode(int respCode) {
        this.respCode = respCode;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public ByteBuffer getBody() {
        return body;
    }

    public void setBody(ByteBuffer body) {
        this.body = body;
    }
}
