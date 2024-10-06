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
public final class StrEncoder extends ByteArrayEncoder {

    private final String str;

    public StrEncoder(String str) {
        super(str == null ? null : str.getBytes(StandardCharsets.UTF_8));
        this.str = str;
    }

    private StrEncoder(byte[] bs) {
        super(bs);
        this.str = new String(bs, StandardCharsets.UTF_8);
    }

    public String getStr() {
        return str;
    }

    @Override
    public String toString() {
        return str;
    }

    public static class Callback extends DecoderCallback<StrEncoder> {

        private StrEncoder r;

        @Override
        protected boolean doDecode(ByteBuffer buffer, int bodyLen, int currentPos) {
            byte[] bytes = parseBytes(buffer, bodyLen, currentPos);
            if (bytes != null) {
                r = new StrEncoder(bytes);
            }
            return true;
        }

        @Override
        protected StrEncoder getResult() {
            return r;
        }
    }

    ;
}
