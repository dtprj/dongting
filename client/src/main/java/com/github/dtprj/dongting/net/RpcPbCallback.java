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

import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.pb.PbException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
class RpcPbCallback implements PbCallback {

    private ReadFrame frame;
    private int bodyStart;
    private int bodyLimit;

    public RpcPbCallback() {
    }

    public void setFrame(ReadFrame frame) {
        this.frame = frame;
        this.bodyStart = -1;
        this.bodyLimit = -1;
    }

    @Override
    public void readInt(int index, int value) {
        switch (index) {
            case Frame.IDX_TYPE:
                frame.setFrameType(value);
                break;
            case Frame.IDX_COMMAND:
                frame.setCommand(value);
                break;
            case Frame.IDX_SEQ:
                frame.setSeq(value);
                break;
            case Frame.IDX_RESP_CODE:
                frame.setRespCode(value);
                break;
            default:
                throw new PbException("readInt " + index);
        }
    }

    @Override
    public void readLong(int index, long value) {
        throw new PbException("readLong " + index);
    }

    @Override
    public void readBytes(int index, ByteBuffer buf) {
        switch (index) {
            case Frame.IDX_MSG: {
                byte[] bs = new byte[buf.remaining()];
                buf.get(bs);
                frame.setMsg(new String(bs, StandardCharsets.UTF_8));
                break;
            }
            case Frame.IDX_BODY: {
                bodyStart = buf.position();
                bodyLimit = bodyStart + buf.remaining();
                break;
            }
            default:
                throw new PbException("readInt " + index);
        }
    }

    public int getBodyStart() {
        return bodyStart;
    }

    public int getBodyLimit() {
        return bodyLimit;
    }
}
