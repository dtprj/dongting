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

import com.github.dtprj.dongting.buf.RefCountByteBuffer;
import com.github.dtprj.dongting.common.DtException;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
@SuppressWarnings("FieldMayBeFinal")
public class RefCountBufWriteFrame extends ByteBufferWriteFrame {
    private RefCountByteBuffer refCountByteBuffer;
    private boolean encodeInvoked;

    public RefCountBufWriteFrame(RefCountByteBuffer refCountByteBuffer) {
        super(refCountByteBuffer == null ? null : refCountByteBuffer.getBuffer());
        this.refCountByteBuffer = refCountByteBuffer;
    }

    @Override
    protected void encodeBody(ByteBuffer buf) {
        if (encodeInvoked) {
            throw new DtException("encode method has been invoked");
        }
        super.encodeBody(buf);
        if (refCountByteBuffer != null) {
            refCountByteBuffer.release();
        }
        encodeInvoked = true;
    }
}
