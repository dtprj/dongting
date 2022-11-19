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
public abstract class CopyWriteFrame extends WriteFrame {

    @Override
    protected final void encodeBody(ByteBuffer buf, ByteBufferPool pool) {
        int s = estimateBodySize();
        if (s > 0) {
            ByteBuffer temp = pool.borrow(s);
            try {
                encodeBody(temp);
                temp.flip();
                PbUtil.writeLengthDelimitedPrefix(buf, Frame.IDX_BODY, temp.remaining());
                buf.put(temp);
            } finally {
                pool.release(temp);
            }
        }
    }

    protected abstract void encodeBody(ByteBuffer tempBuffer);
}
