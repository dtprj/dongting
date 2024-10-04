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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.EncodeContext;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
@SuppressWarnings("FieldMayBeFinal")
public class RefBufWritePacket extends WritePacket {
    private RefBuffer refBuffer;
    private final int size;

    public RefBufWritePacket(RefBuffer refBuffer) {
        this.refBuffer = refBuffer;
        this.size = refBuffer == null ? 0 : refBuffer.getBuffer() == null ? 0 : refBuffer.getBuffer().remaining();
    }

    @Override
    protected void doClean() {
        if (refBuffer != null) {
            refBuffer.release();
            this.refBuffer = null;
        }
    }

    @Override
    protected int calcActualBodySize() {
        return size;
    }

    @Override
    protected boolean encodeBody(EncodeContext context, ByteBuffer dest) {
        return ByteBufferWritePacket.encodeBody(context, refBuffer == null ? null : refBuffer.getBuffer(), dest);
    }
}
