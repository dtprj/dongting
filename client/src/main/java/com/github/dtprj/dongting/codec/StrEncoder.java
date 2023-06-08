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
package com.github.dtprj.dongting.codec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public class StrEncoder implements Encoder<String> {

    private byte[] bytes;
    private int pos;

    public StrEncoder() {
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer buffer, String data) {
        if (data == null) {
            return true;
        }
        byte[] bytes = this.bytes;
        if (bytes == null) {
            bytes = data.getBytes(StandardCharsets.UTF_8);
            this.bytes = bytes;
        }
        int totalLen = bytes.length;
        if (totalLen == 0) {
            return true;
        }
        int pos = this.pos;
        int len = Math.min(buffer.remaining(), totalLen - pos);
        buffer.put(bytes, pos, len);
        pos += len;
        this.pos = pos;
        return pos >= bytes.length;
    }

    @Override
    public int actualSize(String data) {
        if (data == null) {
            return 0;
        }
        bytes = data.getBytes(StandardCharsets.UTF_8);
        return bytes.length;
    }
}
