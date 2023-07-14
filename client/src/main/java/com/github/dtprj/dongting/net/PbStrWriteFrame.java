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

import com.github.dtprj.dongting.codec.PbUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public class PbStrWriteFrame extends SmallNoCopyWriteFrame {

    private final byte[] bytes;

    public PbStrWriteFrame(String value) {
        if (value != null) {
            bytes = value.getBytes(StandardCharsets.UTF_8);
        } else {
            bytes = null;
        }
    }

    @Override
    protected void encodeBody(ByteBuffer buf) {
        PbUtil.writeLengthDelimitedPrefix(buf, 1, bytes.length);
        buf.put(bytes);
    }

    @Override
    protected int calcActualBodySize() {
        return bytes == null ? 0 : PbUtil.accurateLengthDelimitedSize(1, bytes.length);
    }
}
