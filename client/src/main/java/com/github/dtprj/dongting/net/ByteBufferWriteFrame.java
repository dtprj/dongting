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

import com.github.dtprj.dongting.pb.PbUtil;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class ByteBufferWriteFrame extends WriteFrame {
    private ByteBuffer body;
    private int bodySize = -1;

    public ByteBufferWriteFrame(ByteBuffer body) {
        this.body = body;
    }

    @Override
    protected int bodySize() {
        int bodySize = this.bodySize;
        if (bodySize == -1) {
            ByteBuffer body = this.body;
            bodySize = body == null ? 0 : body.remaining();
            this.bodySize = bodySize;
        }
        return bodySize;
    }

    @Override
    protected void encodeBody(ByteBuffer buf) {
        int bs = bodySize();
        if (bs > 0) {
            PbUtil.writeLengthDelimitedPrefix(buf, Frame.IDX_BODY, bs);
            body.mark();
            buf.put(body);
            body.reset();
            body = null;
        }
    }
}
